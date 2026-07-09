"""
CDC Sync (Change Data Capture)
------------------------------
Lightweight incremental sync using timestamp/sequence watermarks.
Polls Postgres for rows changed since last sync and applies them to Neo4j.

For 50M+ row tables at ERP change velocity (not millions of events/sec),
this is far simpler than Kafka + Debezium while being equally correct.

Requires tables to have an `updated_at` timestamp column OR
a monotonically increasing `id` for append-only tables.

Usage:
    python -m erp2neo4j.sync.cdc_sync --mapping config/mapping.yaml
"""
import json
import os
import time
from datetime import datetime, timezone
from dotenv import load_dotenv
import click
import psycopg2

from core.mapping_engine import MappingEngine, NodeDef
from etl.extractor import Extractor
from etl.transformer import NodeTransformer
from etl.loader import Loader
from utils.logger import get_logger

load_dotenv()
log = get_logger(__name__)

WATERMARK_FILE = "config/cdc_watermarks.json"
POLL_INTERVAL = int(os.getenv("CDC_POLL_INTERVAL", 10))


class WatermarkStore:
    """Persists last-sync timestamps per table to disk."""

    def __init__(self, path: str = WATERMARK_FILE):
        self.path = path
        self.watermarks = {}
        self._load()

    def _load(self):
        if os.path.exists(self.path):
            with open(self.path) as f:
                self.watermarks = json.load(f)

    def save(self):
        os.makedirs(os.path.dirname(self.path) or ".", exist_ok=True)
        with open(self.path, "w") as f:
            json.dump(self.watermarks, f, indent=2, default=str)

    def get(self, table: str, default=None):
        return self.watermarks.get(table, default)

    def set(self, table: str, value):
        self.watermarks[table] = str(value)
        self.save()


class CDCSync:
    def __init__(self, mapping_path: str):
        self.mapping = MappingEngine(mapping_path).load()
        self.pg_url = os.getenv("PG_URL")
        self.neo4j_uri = os.getenv("NEO4J_URI", "bolt://localhost:7687")
        self.neo4j_user = os.getenv("NEO4J_USER", "neo4j")
        self.neo4j_pass = os.getenv("NEO4J_PASSWORD", "password")
        self.watermarks = WatermarkStore()
        self.extractor = None
        self.loader = None

    def connect(self):
        self.extractor = Extractor(self.pg_url)
        self.extractor.connect()
        self.loader = Loader(self.neo4j_uri, self.neo4j_user, self.neo4j_pass)

    def close(self):
        if self.extractor:
            self.extractor.close()
        if self.loader:
            self.loader.close()

    def _get_watermark_column(self, table: str) -> str | None:
        """
        Detect the best watermark column for a table.
        Prefers updated_at > modified_at > created_at > id.
        """
        candidates = ["updated_at", "modified_at", "last_modified", "created_at"]
        conn = psycopg2.connect(self.pg_url)
        with conn.cursor() as cur:
            cur.execute("""
                SELECT column_name FROM information_schema.columns
                WHERE table_name = %s AND table_schema = 'public'
            """, (table,))
            cols = {r[0] for r in cur.fetchall()}
        conn.close()

        for c in candidates:
            if c in cols:
                return c

        # Fallback: use PK for append-only detection
        node_def = self.mapping.get_node_def(table)
        if node_def:
            return node_def.id_field

        return None

    def sync_table(self, node_def: NodeDef) -> int:
        watermark_col = self._get_watermark_column(node_def.table)
        if not watermark_col:
            log.warning(f"No watermark column for {node_def.table}, skipping CDC")
            return 0

        last_value = self.watermarks.get(node_def.table)
        if not last_value:
            log.info(f"{node_def.table}: no watermark, doing full resync of table")
            last_value = "1970-01-01T00:00:00"

        transformer = NodeTransformer(node_def)
        new_max = last_value
        total_synced = 0

        all_fields = [node_def.id_field] + node_def.properties
        for batch in self.extractor.stream_since_watermark(
            node_def.table, watermark_col, last_value, columns=all_fields
        ):
            if not batch:
                continue

            transformed = transformer.transform_batch(batch)
            self.loader.load_nodes(node_def, transformed)
            total_synced += len(batch)

            # Track the max watermark seen in this batch
            max_in_batch = max(
                str(r.get(watermark_col, last_value)) for r in batch
            )
            if max_in_batch > new_max:
                new_max = max_in_batch

        if total_synced > 0:
            self.watermarks.set(node_def.table, new_max)
            log.info(
                f"[cyan]{node_def.table}[/cyan]: synced {total_synced:,} rows "
                f"(watermark → {new_max})"
            )

        return total_synced

    def handle_deletes(self, node_def: NodeDef):
        """
        Handle soft deletes: if table has a `deleted_at` column,
        find newly deleted rows and DETACH DELETE them from Neo4j.
        """
        conn = psycopg2.connect(self.pg_url)
        has_deleted_at = False
        with conn.cursor() as cur:
            cur.execute("""
                SELECT column_name FROM information_schema.columns
                WHERE table_name = %s AND column_name = 'deleted_at'
            """, (node_def.table,))
            has_deleted_at = cur.fetchone() is not None

        if not has_deleted_at:
            conn.close()
            return

        last_del_wm = self.watermarks.get(f"{node_def.table}_deletes", "1970-01-01")
        with conn.cursor() as cur:
            cur.execute(
                f'SELECT "{node_def.id_field}" FROM "{node_def.table}" '
                f'WHERE deleted_at > %s ORDER BY deleted_at',
                (last_del_wm,)
            )
            deleted_ids = [r[0] for r in cur.fetchall()]

        conn.close()

        for id_val in deleted_ids:
            self.loader.delete_node(node_def.label, node_def.id_field, id_val)

        if deleted_ids:
            self.watermarks.set(
                f"{node_def.table}_deletes",
                datetime.now(timezone.utc).isoformat()
            )
            log.info(f"[red]Deleted {len(deleted_ids)} {node_def.label} nodes[/red]")

    def run_once(self) -> int:
        total = 0
        for node_def in self.mapping.nodes:
            total += self.sync_table(node_def)
            self.handle_deletes(node_def)
        return total

    def run_continuous(self):
        log.info(f"Starting CDC sync loop (poll interval: {POLL_INTERVAL}s)")
        while True:
            try:
                synced = self.run_once()
                if synced:
                    log.info(f"Sync cycle complete: {synced:,} rows updated")
                else:
                    log.debug("No changes detected")
            except Exception as e:
                log.error(f"Sync error: {e}", exc_info=True)
            time.sleep(POLL_INTERVAL)


@click.command()
@click.option("--mapping", default="config/mapping.yaml")
@click.option("--once", is_flag=True, help="Run one sync cycle and exit")
def main(mapping, once):
    """Run CDC sync: keep Neo4j in sync with PostgreSQL changes."""
    sync = CDCSync(mapping)
    sync.connect()
    try:
        if once:
            synced = sync.run_once()
            log.info(f"Done. {synced:,} rows synced.")
        else:
            sync.run_continuous()
    finally:
        sync.close()


if __name__ == "__main__":
    main()
