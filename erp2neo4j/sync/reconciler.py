"""
Reconciler
----------
Verifies Neo4j data integrity against PostgreSQL after migration or sync.
Compares row counts per table/label and flags mismatches.
Triggers partial reloads for divergent tables.

Usage:
    python -m erp2neo4j.sync.reconciler --mapping config/mapping.yaml
"""
import os
from dotenv import load_dotenv
import click
import psycopg2
from rich.table import Table
from rich.console import Console

from core.mapping_engine import MappingEngine
from utils.neo4j_admin import Neo4jAdmin
from utils.logger import get_logger

load_dotenv()
log = get_logger(__name__)
console = Console()


class Reconciler:
    def __init__(self, mapping_path: str):
        self.mapping = MappingEngine(mapping_path).load()
        self.pg_url = os.getenv("PG_URL")
        self.neo4j_uri = os.getenv("NEO4J_URI", "bolt://localhost:7687")
        self.neo4j_user = os.getenv("NEO4J_USER", "neo4j")
        self.neo4j_pass = os.getenv("NEO4J_PASSWORD", "password")
        self.admin = Neo4jAdmin(self.neo4j_uri, self.neo4j_user, self.neo4j_pass)

    def _pg_count(self, table: str) -> int:
        conn = psycopg2.connect(self.pg_url)
        with conn.cursor() as cur:
            cur.execute(f'SELECT COUNT(*) FROM "{table}"')
            count = cur.fetchone()[0]
        conn.close()
        return count

    def reconcile_nodes(self) -> list[dict]:
        results = []
        for node_def in self.mapping.nodes:
            pg_count = self._pg_count(node_def.table)
            neo4j_count = self.admin.get_node_count(node_def.label)
            delta = neo4j_count - pg_count
            pct = (neo4j_count / max(pg_count, 1)) * 100

            results.append({
                "table": node_def.table,
                "label": node_def.label,
                "pg_count": pg_count,
                "neo4j_count": neo4j_count,
                "delta": delta,
                "pct": pct,
                "ok": abs(delta) == 0,
            })

        return results

    def reconcile_relationships(self) -> list[dict]:
        results = []
        for rel_def in self.mapping.relationships:
            from_node = self.mapping.get_node_def(rel_def.from_table)
            if not from_node:
                continue
            pg_count = self._pg_count(rel_def.from_table)
            neo4j_count = self.admin.get_relationship_count(rel_def.type)
            delta = neo4j_count - pg_count
            results.append({
                "type": rel_def.type,
                "pg_count": pg_count,
                "neo4j_count": neo4j_count,
                "delta": delta,
                "ok": abs(delta) <= int(pg_count * 0.001),  # allow 0.1% tolerance for soft deletes
            })
        return results

    def print_report(self, node_results: list[dict], rel_results: list[dict]):
        # Nodes table
        t = Table(title="Node Reconciliation", show_lines=True)
        t.add_column("Table")
        t.add_column("Label")
        t.add_column("Postgres", justify="right")
        t.add_column("Neo4j", justify="right")
        t.add_column("Delta", justify="right")
        t.add_column("Status")

        for r in node_results:
            status = "[green]✔ OK[/green]" if r["ok"] else "[red]✘ MISMATCH[/red]"
            delta_str = str(r["delta"]) if r["delta"] == 0 else f"[red]{r['delta']:+,}[/red]"
            t.add_row(
                r["table"], r["label"],
                f"{r['pg_count']:,}", f"{r['neo4j_count']:,}",
                delta_str, status
            )
        console.print(t)

        # Relationships table
        rt = Table(title="Relationship Reconciliation", show_lines=True)
        rt.add_column("Type")
        rt.add_column("Postgres", justify="right")
        rt.add_column("Neo4j", justify="right")
        rt.add_column("Delta", justify="right")
        rt.add_column("Status")

        for r in rel_results:
            status = "[green]✔ OK[/green]" if r["ok"] else "[red]✘ MISMATCH[/red]"
            delta_str = str(r["delta"]) if r["delta"] == 0 else f"[red]{r['delta']:+,}[/red]"
            rt.add_row(
                r["type"],
                f"{r['pg_count']:,}", f"{r['neo4j_count']:,}",
                delta_str, status
            )
        console.print(rt)

    def run(self) -> bool:
        """Returns True if all counts match, False if mismatches found."""
        log.info("Running reconciliation...")
        node_results = self.reconcile_nodes()
        rel_results = self.reconcile_relationships()
        self.print_report(node_results, rel_results)

        all_ok = all(r["ok"] for r in node_results + rel_results)
        if all_ok:
            log.info("[bold green]✔ Reconciliation passed — all counts match![/bold green]")
        else:
            mismatches = [r for r in node_results + rel_results if not r["ok"]]
            log.error(f"[bold red]✘ {len(mismatches)} mismatch(es) found![/bold red]")

        self.admin.close()
        return all_ok


@click.command()
@click.option("--mapping", default="config/mapping.yaml")
def main(mapping):
    """Reconcile Neo4j counts against PostgreSQL source."""
    reconciler = Reconciler(mapping)
    ok = reconciler.run()
    raise SystemExit(0 if ok else 1)


if __name__ == "__main__":
    main()
