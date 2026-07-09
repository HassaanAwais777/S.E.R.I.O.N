"""
Multi-Tenant Reconciler + Super Admin
--------------------------------------
Two tools in one file:

1. MultiTenantReconciler
   Runs count reconciliation for all tenants.
   Reports which companies are in sync.

2. SuperAdminQuery
   Allows a super-admin to query ACROSS companies (read-only).
   Uses Neo4j's USE clause to switch databases per query.
   NEVER mixes data between companies — queries are run separately
   and results are aggregated in Python, not in Cypher.

Usage:
    # Reconcile all
    python -m erp2neo4j.tenancy.reconciler

    # Super admin: count all customers across all companies
    python -m erp2neo4j.tenancy.reconciler --admin-query "MATCH (n:Customer) RETURN count(n)"
"""
import os
import click
import psycopg2
from rich.table import Table
from rich.console import Console
from dotenv import load_dotenv
from neo4j import GraphDatabase

from tenancy.registry import TenantRegistry, TenantConfig, Neo4jConfig
from tenancy.neo4j_manager import TenantNeo4jManager
from core.mapping_engine import MappingEngine
from utils.logger import get_logger

load_dotenv()
log = get_logger(__name__)
console = Console()


class MultiTenantReconciler:
    def __init__(self, registry_path: str = "config/tenants.yaml",
                 company_filter: str = None):
        self.registry = TenantRegistry(registry_path).load()
        self.company_filter = company_filter

    def _pg_count(self, pg_url: str, table: str) -> int:
        conn = psycopg2.connect(pg_url)
        with conn.cursor() as cur:
            cur.execute(f'SELECT COUNT(*) FROM "{table}"')
            count = cur.fetchone()[0]
        conn.close()
        return count

    def reconcile_tenant(self, tenant: TenantConfig) -> list[dict]:
        mapping = MappingEngine(tenant.mapping_file).load()
        manager = TenantNeo4jManager(tenant, self.registry.neo4j)
        results = []

        try:
            for node_def in mapping.nodes:
                pg_count = self._pg_count(tenant.pg_url, node_def.table)
                neo4j_count = manager.node_count(node_def.label)
                delta = neo4j_count - pg_count
                results.append({
                    "company": tenant.name,
                    "table": node_def.table,
                    "label": node_def.label,
                    "pg": pg_count,
                    "neo4j": neo4j_count,
                    "delta": delta,
                    "ok": abs(delta) == 0,
                })
        finally:
            manager.close()

        return results

    def run(self):
        tenants = self.registry.active_tenants()
        if self.company_filter:
            tenants = [t for t in tenants if t.company_id == self.company_filter]

        all_results = []
        for tenant in tenants:
            log.info(f"Reconciling: {tenant.name}")
            try:
                results = self.reconcile_tenant(tenant)
                all_results.extend(results)
            except Exception as e:
                log.error(f"{tenant.company_id}: reconcile failed — {e}")

        # Print table
        t = Table(title="Multi-Tenant Reconciliation", show_lines=True)
        t.add_column("Company")
        t.add_column("Label")
        t.add_column("Postgres", justify="right")
        t.add_column("Neo4j", justify="right")
        t.add_column("Delta", justify="right")
        t.add_column("Status")

        for r in all_results:
            ok = r["ok"]
            t.add_row(
                r["company"], r["label"],
                f"{r['pg']:,}", f"{r['neo4j']:,}",
                str(r["delta"]) if ok else f"[red]{r['delta']:+,}[/red]",
                "[green]✔[/green]" if ok else "[red]✘[/red]"
            )

        console.print(t)
        mismatches = [r for r in all_results if not r["ok"]]
        if mismatches:
            log.error(f"[red]{len(mismatches)} mismatch(es) found[/red]")
        else:
            log.info("[bold green]✔ All counts match across all tenants![/bold green]")

        return len(mismatches) == 0


class SuperAdminQuery:
    """
    Super-admin read access across all company databases.
    Runs the same Cypher on each tenant's database SEPARATELY
    and aggregates results in Python.
    This is the ONLY safe way to do cross-company queries —
    never mix company data in a single Cypher query.
    """

    def __init__(self, registry_path: str = "config/tenants.yaml"):
        self.registry = TenantRegistry(registry_path).load()

    def run_on_all(self, cypher: str) -> dict[str, list[dict]]:
        """
        Run a read-only Cypher query on every active tenant's database.
        Returns {company_id: [result_rows]}.
        """
        results = {}
        for tenant in self.registry.active_tenants():
            manager = TenantNeo4jManager(tenant, self.registry.neo4j)
            try:
                with manager.session() as s:
                    r = s.run(cypher)
                    results[tenant.company_id] = [dict(row) for row in r]
            except Exception as e:
                log.error(f"[{tenant.company_id}] query failed: {e}")
                results[tenant.company_id] = []
            finally:
                manager.close()
        return results

    def run_on_one(self, company_id: str, cypher: str) -> list[dict]:
        """Query a specific company's database."""
        tenant = self.registry.get(company_id)
        if not tenant:
            raise ValueError(f"Unknown company: {company_id}")
        manager = TenantNeo4jManager(tenant, self.registry.neo4j)
        try:
            with manager.session() as s:
                return [dict(row) for row in s.run(cypher)]
        finally:
            manager.close()

    def aggregate_count(self, cypher: str) -> dict:
        """
        Run a COUNT query across all companies.
        Returns {company_id: count, ..., "__total__": total}
        """
        all_results = self.run_on_all(cypher)
        counts = {}
        total = 0
        for company_id, rows in all_results.items():
            count = rows[0].get("count(n)", 0) if rows else 0
            counts[company_id] = count
            total += count
        counts["__total__"] = total
        return counts


@click.command()
@click.option("--registry", default="config/tenants.yaml")
@click.option("--company", default=None)
@click.option("--admin-query", default=None,
              help="Run a Cypher query across all company databases (super-admin)")
def main(registry, company, admin_query):
    """Reconcile or query across all tenant Neo4j databases."""
    if admin_query:
        sa = SuperAdminQuery(registry)
        results = sa.run_on_all(admin_query)
        for cid, rows in results.items():
            console.print(f"\n[bold cyan]{cid}[/bold cyan]")
            for row in rows[:20]:   # limit output
                console.print(f"  {row}")
    else:
        reconciler = MultiTenantReconciler(registry, company_filter=company)
        ok = reconciler.run()
        raise SystemExit(0 if ok else 1)


if __name__ == "__main__":
    main()
