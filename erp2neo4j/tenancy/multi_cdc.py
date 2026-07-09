"""
Multi-Tenant CDC Sync
---------------------
Runs incremental sync for ALL active tenants continuously.
Each tenant has its own watermark store and sync loop.
Tenants sync in parallel — one slow company doesn't block others.

Usage:
    python -m erp2neo4j.tenancy.multi_cdc

    # Specific company
    python -m erp2neo4j.tenancy.multi_cdc --company company_acme

    # One shot (for cron jobs)
    python -m erp2neo4j.tenancy.multi_cdc --once
"""
import os
import time
import json
import threading
from datetime import datetime, timezone
from dotenv import load_dotenv
import click
import psycopg2

from tenancy.registry import TenantRegistry, TenantConfig
from tenancy.neo4j_manager import TenantNeo4jManager
from tenancy.tenant_loader import TenantLoader
from core.mapping_engine import MappingEngine
from etl.extractor import Extractor
from etl.transformer import NodeTransformer
from utils.logger import get_logger

load_dotenv()
log = get_logger(__name__)

POLL_INTERVAL = int(os.getenv("CDC_POLL_INTERVAL", 10))
WATERMARK_DIR = "config/watermarks"


class TenantWatermarkStore:
    """Per-tenant watermark file: config/watermarks/{company_id}.json"""

    def __init__(self, company_id: str):
        os.makedirs(WATERMARK_DIR, exist_ok=True)
        self.path = os.path.join(WATERMARK_DIR, f"{company_id}.json")
        self.data: dict = {}
        self._load()

    def _load(self):
        if os.path.exists(self.path):
            with open(self.path) as f:
                self.data = json.load(f)

    def _save(self):
        with open(self.path, "w") as f:
            json.dump(self.data, f, indent=2, default=str)

    def get(self, table: str, default="1970-01-01T00:00:00"):
        return self.data.get(table, default)

    def set(self, table: str, value):
        self.data[table] = str(value)
        self._save()


class TenantCDCSync:
    """
    Incremental sync for a single tenant.
    Each run polls all tables for rows changed since last watermark.
    """

    def __init__(self, tenant: TenantConfig, registry: TenantRegistry):
        self.tenant = tenant
        self.registry = registry
        self.watermarks = TenantWatermarkStore(tenant.company_id)

    def _detect_watermark_col(self, table: str, pg_url: str) -> str | None:
        candidates = ["updated_at", "modified_at", "last_modified", "created_at"]
        conn = psycopg2.connect(pg_url)
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
        return None

    def sync_once(self) -> int:
        tid = self.tenant.company_id
        mapping = MappingEngine(self.tenant.mapping_file).load()
        manager = TenantNeo4jManager(self.tenant, self.registry.neo4j)
        loader = TenantLoader(manager)
        extractor = Extractor(self.tenant.pg_url)
        extractor.connect()

        total = 0
        try:
            for node_def in mapping.nodes:
                wm_col = self._detect_watermark_col(
                    node_def.table, self.tenant.pg_url
                )
                if not wm_col:
                    continue

                last = self.watermarks.get(node_def.table)
                transformer = NodeTransformer(node_def)
                new_max = last
                synced = 0

                all_fields = [node_def.id_field] + node_def.properties
                for batch in extractor.stream_since_watermark(
                    node_def.table, wm_col, last, columns=all_fields
                ):
                    if not batch:
                        continue
                    transformed = transformer.transform_batch(batch)
                    loader.load_nodes(node_def, transformed)
                    synced += len(batch)
                    max_val = max(str(r.get(wm_col, last)) for r in batch)
                    if max_val > new_max:
                        new_max = max_val

                if synced:
                    self.watermarks.set(node_def.table, new_max)
                    log.info(
                        f"[{tid}] [cyan]{node_def.table}[/cyan]: "
                        f"synced {synced:,} rows"
                    )
                    total += synced

                # Handle soft deletes
                self._handle_deletes(node_def, manager)

        finally:
            extractor.close()
            manager.close()

        return total

    def _handle_deletes(self, node_def, manager: TenantNeo4jManager):
        conn = psycopg2.connect(self.tenant.pg_url)
        with conn.cursor() as cur:
            cur.execute("""
                SELECT column_name FROM information_schema.columns
                WHERE table_name = %s AND column_name = 'deleted_at'
            """, (node_def.table,))
            has_deleted = cur.fetchone() is not None
        if not has_deleted:
            conn.close()
            return

        last_del = self.watermarks.get(
            f"{node_def.table}__deletes", "1970-01-01"
        )
        with conn.cursor() as cur:
            cur.execute(
                f'SELECT "{node_def.id_field}" FROM "{node_def.table}" '
                f'WHERE deleted_at > %s',
                (last_del,)
            )
            deleted_ids = [r[0] for r in cur.fetchall()]
        conn.close()

        for id_val in deleted_ids:
            manager.delete_node(node_def.label, node_def.id_field, id_val)

        if deleted_ids:
            self.watermarks.set(
                f"{node_def.table}__deletes",
                datetime.now(timezone.utc).isoformat()
            )
            log.info(
                f"[{self.tenant.company_id}] "
                f"[red]Deleted {len(deleted_ids)} {node_def.label}(s)[/red]"
            )


class MultiTenantCDC:
    """Runs TenantCDCSync for all active tenants, in parallel threads."""

    def __init__(self, registry_path: str = "config/tenants.yaml",
                 company_filter: str = None):
        self.registry = TenantRegistry(registry_path).load()
        self.company_filter = company_filter

    def _get_tenants(self) -> list[TenantConfig]:
        tenants = self.registry.active_tenants()
        if self.company_filter:
            tenants = [t for t in tenants if t.company_id == self.company_filter]
        return tenants

    def run_once(self):
        tenants = self._get_tenants()
        threads = []
        for tenant in tenants:
            sync = TenantCDCSync(tenant, self.registry)
            t = threading.Thread(
                target=self._safe_sync,
                args=(sync,),
                name=tenant.company_id,
                daemon=True
            )
            threads.append(t)
            t.start()
        for t in threads:
            t.join()

    def _safe_sync(self, sync: TenantCDCSync):
        try:
            n = sync.sync_once()
            if n:
                log.info(f"[{sync.tenant.company_id}] Cycle done: {n:,} rows")
        except Exception as e:
            log.error(f"[{sync.tenant.company_id}] Sync error: {e}", exc_info=True)

    def run_continuous(self):
        log.info(
            f"[bold]Multi-tenant CDC started[/bold] "
            f"(poll interval: {POLL_INTERVAL}s)"
        )
        while True:
            self.run_once()
            time.sleep(POLL_INTERVAL)


@click.command()
@click.option("--registry", default="config/tenants.yaml")
@click.option("--company", default=None, help="Specific company_id to sync")
@click.option("--once", is_flag=True, help="Run one sync cycle and exit")
def main(registry, company, once):
    """Multi-tenant CDC: keep all company Neo4j databases in sync."""
    cdc = MultiTenantCDC(registry, company_filter=company)
    if once:
        cdc.run_once()
    else:
        cdc.run_continuous()


if __name__ == "__main__":
    main()
