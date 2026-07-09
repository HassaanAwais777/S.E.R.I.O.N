"""
Schema Introspector
-------------------
Connects to PostgreSQL, reads information_schema, and auto-generates
a mapping.yaml file that can be reviewed and edited before running the ETL.

Usage:
    python -m erp2neo4j.core.introspector \
        --pg-url "postgresql://user:pass@host/db" \
        --output config/mapping.yaml
"""
import json
import os
import click
import psycopg2
import yaml
from dotenv import load_dotenv
from utils.logger import get_logger

load_dotenv()
log = get_logger(__name__)


class SchemaIntrospector:
    def __init__(self, pg_url: str):
        self.pg_url = pg_url
        self.conn = None

    def connect(self):
        self.conn = psycopg2.connect(self.pg_url)
        log.info("Connected to PostgreSQL")

    def close(self):
        if self.conn:
            self.conn.close()

    def get_tables(self, schema: str = "public") -> list[dict]:
        """Return all user tables with row estimates."""
        with self.conn.cursor() as cur:
            cur.execute("""
                SELECT
                    t.table_name,
                    COALESCE(s.n_live_tup, 0) AS estimated_rows
                FROM information_schema.tables t
                LEFT JOIN pg_stat_user_tables s ON s.relname = t.table_name
                WHERE t.table_schema = %s
                  AND t.table_type = 'BASE TABLE'
                ORDER BY estimated_rows DESC
            """, (schema,))
            return [{"table": r[0], "estimated_rows": r[1]} for r in cur.fetchall()]

    def get_columns(self, table: str, schema: str = "public") -> list[dict]:
        """Return all columns for a table with types and nullability."""
        with self.conn.cursor() as cur:
            cur.execute("""
                SELECT
                    column_name,
                    data_type,
                    is_nullable,
                    column_default
                FROM information_schema.columns
                WHERE table_schema = %s AND table_name = %s
                ORDER BY ordinal_position
            """, (schema, table))
            return [
                {"name": r[0], "type": r[1], "nullable": r[2] == "YES", "default": r[3]}
                for r in cur.fetchall()
            ]

    def get_primary_keys(self, schema: str = "public") -> dict[str, str]:
        """Return {table: pk_column} for all tables."""
        with self.conn.cursor() as cur:
            cur.execute("""
                SELECT
                    tc.table_name,
                    kcu.column_name
                FROM information_schema.table_constraints tc
                JOIN information_schema.key_column_usage kcu
                    ON tc.constraint_name = kcu.constraint_name
                    AND tc.table_schema = kcu.table_schema
                WHERE tc.constraint_type = 'PRIMARY KEY'
                  AND tc.table_schema = %s
            """, (schema,))
            return {r[0]: r[1] for r in cur.fetchall()}

    def get_foreign_keys(self, schema: str = "public") -> list[dict]:
        """Return all FK relationships between tables."""
        with self.conn.cursor() as cur:
            cur.execute("""
                SELECT
                    tc.table_name       AS from_table,
                    kcu.column_name     AS from_column,
                    ccu.table_name      AS to_table,
                    ccu.column_name     AS to_column,
                    tc.constraint_name
                FROM information_schema.table_constraints tc
                JOIN information_schema.key_column_usage kcu
                    ON tc.constraint_name = kcu.constraint_name
                    AND tc.table_schema = kcu.table_schema
                JOIN information_schema.constraint_column_usage ccu
                    ON ccu.constraint_name = tc.constraint_name
                    AND ccu.table_schema = tc.table_schema
                WHERE tc.constraint_type = 'FOREIGN KEY'
                  AND tc.table_schema = %s
                ORDER BY tc.table_name
            """, (schema,))
            return [
                {
                    "from_table": r[0], "from_column": r[1],
                    "to_table": r[2],   "to_column": r[3],
                    "constraint": r[4]
                }
                for r in cur.fetchall()
            ]

    def detect_junction_tables(self, tables: list[dict], fk_map: list[dict]) -> set[str]:
        """
        Heuristic: a table is a junction table if it has exactly 2 FKs
        and its non-FK columns are only payload (qty, price, etc.).
        These become RELATIONSHIPS in Neo4j, not nodes.
        """
        from collections import Counter
        fk_counts = Counter(fk["from_table"] for fk in fk_map)
        pks = self.get_primary_keys()

        junction = set()
        for table_info in tables:
            t = table_info["table"]
            if fk_counts.get(t, 0) == 2:
                cols = self.get_columns(t)
                pk = pks.get(t)
                non_fk_cols = [
                    c for c in cols
                    if c["name"] != pk
                    and not any(fk["from_column"] == c["name"] and fk["from_table"] == t for fk in fk_map)
                ]
                # If only a handful of payload columns, treat as junction
                if len(non_fk_cols) <= 5:
                    junction.add(t)
                    log.info(f"[yellow]Junction table detected:[/yellow] {t}")
        return junction

    def _to_label(self, table_name: str) -> str:
        """Convert snake_case table name to PascalCase Neo4j label."""
        return "".join(word.capitalize() for word in table_name.split("_"))

    def _to_rel_type(self, from_table: str, to_table: str, via: str = None) -> str:
        """Generate a readable relationship type."""
        # Common ERP verb mappings
        verb_map = {
            ("orders", "customers"):       "PLACED_BY",
            ("order_items", "orders"):     "BELONGS_TO",
            ("order_items", "products"):   "CONTAINS",
            ("products", "categories"):    "IN_CATEGORY",
            ("products", "suppliers"):     "SUPPLIED_BY",
            ("invoices", "orders"):        "GENERATED_FROM",
            ("employees", "departments"):  "WORKS_IN",
            ("purchases", "suppliers"):    "FROM_SUPPLIER",
        }
        key = (from_table, to_table)
        if key in verb_map:
            return verb_map[key]
        # Fallback: HAS_<TO_TABLE>
        return f"HAS_{to_table.upper().rstrip('S')}"

    def build_mapping(self, schema: str = "public") -> dict:
        """Full introspection → mapping dict."""
        log.info("Introspecting schema...")
        tables = self.get_tables(schema)
        pks = self.get_primary_keys(schema)
        fks = self.get_foreign_keys(schema)
        junction_tables = self.detect_junction_tables(tables, fks)

        nodes = []
        relationships = []
        junction_relationships = []

        for t in tables:
            table = t["table"]
            if table in junction_tables:
                continue  # Will become a relationship

            pk = pks.get(table)
            if not pk:
                log.warning(f"No PK found for {table}, skipping")
                continue

            cols = self.get_columns(table, schema)
            props = [c["name"] for c in cols if c["name"] != pk]

            nodes.append({
                "table": table,
                "label": self._to_label(table),
                "id_field": pk,
                "properties": props,
                "indexed_properties": [],   # user can add
                "estimated_rows": t["estimated_rows"],
            })

        # Direct FK relationships (non-junction)
        processed_junctions = {}
        for fk in fks:
            from_t = fk["from_table"]
            to_t = fk["to_table"]

            if from_t in junction_tables:
                # Accumulate junction FKs to build relationship
                processed_junctions.setdefault(from_t, []).append(fk)
                continue

            if from_t not in {n["table"] for n in nodes}:
                continue
            if to_t not in {n["table"] for n in nodes}:
                continue

            relationships.append({
                "type": self._to_rel_type(from_t, to_t),
                "from": {"table": from_t, "fk": fk["from_column"]},
                "to":   {"table": to_t,   "pk": fk["to_column"]},
                "properties": [],
            })

        # Junction table relationships
        for jt, jfks in processed_junctions.items():
            if len(jfks) == 2:
                from_fk, to_fk = jfks[0], jfks[1]
                cols = self.get_columns(jt, schema)
                fk_cols = {f["from_column"] for f in jfks}
                payload_props = [c["name"] for c in cols if c["name"] not in fk_cols]

                junction_relationships.append({
                    "type": self._to_rel_type(from_fk["to_table"], to_fk["to_table"], via=jt),
                    "junction_table": jt,
                    "from": {"table": from_fk["to_table"], "fk": from_fk["from_column"]},
                    "to":   {"table": to_fk["to_table"],   "fk": to_fk["from_column"]},
                    "properties": payload_props,
                })

        mapping = {
            "version": "1.0",
            "source": {"schema": schema},
            "nodes": nodes,
            "relationships": relationships,
            "junction_relationships": junction_relationships,
        }

        log.info(
            f"Introspection complete: "
            f"[cyan]{len(nodes)} nodes[/cyan], "
            f"[cyan]{len(relationships)} relationships[/cyan], "
            f"[cyan]{len(junction_relationships)} junction relationships[/cyan]"
        )
        return mapping

    def save_mapping(self, mapping: dict, output_path: str):
        os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)
        with open(output_path, "w") as f:
            yaml.dump(mapping, f, default_flow_style=False, sort_keys=False, allow_unicode=True)
        log.info(f"Mapping saved to [bold]{output_path}[/bold]")

    def save_schema_snapshot(self, output_path: str = "config/schema_snapshot.json"):
        """Save schema snapshot for drift detection on future runs."""
        pks = self.get_primary_keys()
        fks = self.get_foreign_keys()
        snapshot = {"primary_keys": pks, "foreign_keys": fks}
        os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)
        with open(output_path, "w") as f:
            json.dump(snapshot, f, indent=2)
        log.info(f"Schema snapshot saved to {output_path}")


@click.command()
@click.option("--pg-url", envvar="PG_URL", required=True, help="PostgreSQL connection URL")
@click.option("--schema", default="public", help="Postgres schema name")
@click.option("--output", default="config/mapping.yaml", help="Output mapping file path")
def main(pg_url, schema, output):
    """Introspect PostgreSQL schema and generate a mapping config."""
    introspector = SchemaIntrospector(pg_url)
    try:
        introspector.connect()
        mapping = introspector.build_mapping(schema)
        introspector.save_mapping(mapping, output)
        introspector.save_schema_snapshot()
        log.info("[bold green]✔ Schema introspection complete![/bold green]")
        log.info(f"Review and edit [bold]{output}[/bold] before running the ETL.")
    finally:
        introspector.close()


if __name__ == "__main__":
    main()
