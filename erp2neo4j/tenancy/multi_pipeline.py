"""
Multi-Tenant Pipeline
---------------------
Orchestrates the full migration for ALL tenants (companies) in parallel.
Each tenant runs in its own thread with its own isolated Neo4j database.

Usage:
    # Run all active tenants
    python -m erp2neo4j.tenancy.multi_pipeline

    # Run a specific company only
    python -m erp2neo4j.tenancy.multi_pipeline --company company_acme

    # Run introspection for all tenants (generates mapping files)
    python -m erp2neo4j.tenancy.multi_pipeline --introspect-only
"""
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dotenv import load_dotenv
import click
from rich.console import Console
from rich.table import Table
from rich.live import Live
from rich.panel import Panel

from tenancy.registry import TenantRegistry, TenantConfig
from tenancy.neo4j_manager import TenantNeo4jManager
from tenancy.tenant_loader import TenantLoader
from core.introspector import SchemaIntrospector
from core.mapping_engine import MappingEngine
from core.quality_gate import QualityGate
from etl.extractor import Extractor
from etl.transformer import NodeTransformer, RelTransformer, JunctionRelTransformer
from utils.logger import get_logger

load_dotenv()
log = get_logger(__name__)
console = Console()

BATCH_SIZE = int(os.getenv("BATCH_SIZE", 5000))
CHUNK_SIZE = int(os.getenv("CHUNK_SIZE", 100_000))
NUM_WORKERS = int(os.getenv("NUM_WORKERS", 8))
TENANT_PARALLELISM = int(os.getenv("TENANT_PARALLELISM", 3))  # companies in parallel


class SingleTenantPipeline:
    """
    Full ETL pipeline for ONE tenant (company).
    Runs completely isolated — no shared state with other tenants.
    """

    def __init__(self, tenant: TenantConfig, registry: TenantRegistry):
        self.tenant = tenant
        self.registry = registry
        self.stats = {
            "company_id": tenant.company_id,
            "company_name": tenant.name,
            "nodes": 0,
            "relationships": 0,
            "junction_rels": 0,
            "duration_s": 0,
            "status": "pending",
        }

    def run(self) -> dict:
        start = time.time()
        tid = self.tenant.company_id
        log.info(f"\n[bold cyan]━━━ Starting: {self.tenant.name} ({tid}) ━━━[/bold cyan]")

        # 1. Load mapping
        mapping = MappingEngine(self.tenant.mapping_file).load()

        # 2. Create/verify Neo4j database for this tenant
        manager = TenantNeo4jManager(self.tenant, self.registry.neo4j)
        manager.create_database_if_not_exists()
        manager.setup_constraints(mapping.raw)
        loader = TenantLoader(manager, batch_size=BATCH_SIZE)

        # 3. Create Department nodes
        loader.load_department_nodes()

        # 4. Extractor (Postgres → this tenant's DB)
        extractor = Extractor(self.tenant.pg_url, batch_size=BATCH_SIZE)
        extractor.connect()

        try:
            # ── Phase A: Load all nodes ────────────────────────
            log.info(f"[{tid}] Loading nodes...")
            for node_def in mapping.nodes:
                gate = QualityGate()
                transformer = NodeTransformer(node_def)
                all_fields = [node_def.id_field] + node_def.properties

                chunks = extractor.get_pk_chunks(
                    node_def.table, node_def.id_field, CHUNK_SIZE
                )

                # Parallel chunk loading within this tenant
                with ThreadPoolExecutor(max_workers=NUM_WORKERS) as pool:
                    futures = []
                    for chunk_min, chunk_max in chunks:
                        futures.append(pool.submit(
                            self._load_node_chunk,
                            node_def, chunk_min, chunk_max,
                            all_fields, loader
                        ))
                    for f in as_completed(futures):
                        self.stats["nodes"] += f.result()

                gate.close()

            # ── Phase B: Load relationships ────────────────────
            log.info(f"[{tid}] Loading relationships...")
            for rel_def in mapping.relationships:
                from_node = mapping.get_node_def(rel_def.from_table)
                to_node   = mapping.get_node_def(rel_def.to_table)
                if not from_node or not to_node:
                    continue

                transformer = RelTransformer(rel_def)
                cols = [rel_def.from_fk] + rel_def.properties

                for batch in extractor.stream_table(rel_def.from_table, columns=cols):
                    transformed = transformer.transform_batch(batch)
                    self.stats["relationships"] += loader.load_relationships(
                        rel_def, transformed,
                        from_node.label, to_node.label,
                        from_node.id_field, to_node.id_field
                    )

            # ── Phase C: Junction relationships ───────────────
            log.info(f"[{tid}] Loading junction relationships...")
            for jrel in mapping.junction_relationships:
                from_node = mapping.get_node_def(jrel.from_table)
                to_node   = mapping.get_node_def(jrel.to_table)
                if not from_node or not to_node:
                    continue

                transformer = JunctionRelTransformer(jrel)
                cols = [jrel.from_fk, jrel.to_fk] + jrel.properties

                for batch in extractor.stream_table(jrel.junction_table, columns=cols):
                    transformed = transformer.transform_batch(batch)
                    self.stats["junction_rels"] += loader.load_junction_relationships(
                        jrel, transformed,
                        from_node.label, to_node.label,
                        from_node.id_field, to_node.id_field
                    )

            self.stats["status"] = "✔ done"

        except Exception as e:
            self.stats["status"] = f"✘ failed: {str(e)[:40]}"
            log.error(f"[{tid}] FAILED: {e}", exc_info=True)

        finally:
            extractor.close()
            manager.close()

        self.stats["duration_s"] = round(time.time() - start, 1)
        log.info(
            f"[bold green][{tid}] Complete: "
            f"{self.stats['nodes']:,} nodes, "
            f"{self.stats['relationships']:,} rels "
            f"in {self.stats['duration_s']}s[/bold green]"
        )
        return self.stats

    def _load_node_chunk(self, node_def, chunk_min, chunk_max,
                         all_fields, loader) -> int:
        """Load one PK-range chunk for a node table."""
        extractor = Extractor(self.tenant.pg_url, batch_size=BATCH_SIZE)
        extractor.connect()
        transformer = NodeTransformer(node_def)
        written = 0
        try:
            for batch in extractor.stream_chunk(
                node_def.table, node_def.id_field,
                chunk_min, chunk_max, columns=all_fields
            ):
                transformed = transformer.transform_batch(batch)
                written += loader.load_nodes(node_def, transformed)
        finally:
            extractor.close()
        return written


class MultiTenantPipeline:
    """Runs SingleTenantPipeline for all active tenants in parallel."""

    def __init__(self, registry_path: str = "config/tenants.yaml",
                 company_filter: str = None):
        self.registry = TenantRegistry(registry_path).load()
        self.company_filter = company_filter

    def _get_tenants(self) -> list[TenantConfig]:
        tenants = self.registry.active_tenants()
        if self.company_filter:
            tenants = [t for t in tenants if t.company_id == self.company_filter]
            if not tenants:
                raise ValueError(
                    f"Company '{self.company_filter}' not found or inactive."
                )
        return tenants

    def run_introspection(self):
        """Auto-generate mapping.yaml for each tenant."""
        tenants = self._get_tenants()
        log.info(f"Running introspection for {len(tenants)} tenant(s)...")
        for tenant in tenants:
            log.info(f"[cyan]Introspecting: {tenant.name}[/cyan]")
            os.makedirs(os.path.dirname(tenant.mapping_file), exist_ok=True)
            intro = SchemaIntrospector(tenant.pg_url)
            try:
                intro.connect()
                mapping = intro.build_mapping()
                intro.save_mapping(mapping, tenant.mapping_file)
                intro.save_schema_snapshot(
                    tenant.mapping_file.replace("mapping.yaml", "schema_snapshot.json")
                )
                log.info(f"[green]✔ {tenant.name}: mapping saved[/green]")
            finally:
                intro.close()

    def run(self):
        tenants = self._get_tenants()
        self.registry.validate()

        log.info(
            f"\n[bold]Starting multi-tenant migration[/bold]\n"
            f"Companies: {', '.join(t.company_id for t in tenants)}\n"
            f"Parallelism: {TENANT_PARALLELISM} companies at a time\n"
        )

        all_stats = []
        start = time.time()

        with ThreadPoolExecutor(max_workers=TENANT_PARALLELISM) as pool:
            futures = {
                pool.submit(SingleTenantPipeline(t, self.registry).run): t
                for t in tenants
            }
            for future in as_completed(futures):
                tenant = futures[future]
                try:
                    stats = future.result()
                    all_stats.append(stats)
                except Exception as e:
                    log.error(f"{tenant.company_id} pipeline crashed: {e}")
                    all_stats.append({
                        "company_id": tenant.company_id,
                        "company_name": tenant.name,
                        "status": f"✘ crashed",
                        "nodes": 0, "relationships": 0,
                        "junction_rels": 0, "duration_s": 0,
                    })

        self._print_summary(all_stats, time.time() - start)

    def _print_summary(self, all_stats: list[dict], total_s: float):
        table = Table(title="Multi-Tenant Migration Summary", show_lines=True)
        table.add_column("Company", style="bold")
        table.add_column("Nodes", justify="right")
        table.add_column("Rels", justify="right")
        table.add_column("Junc. Rels", justify="right")
        table.add_column("Time (s)", justify="right")
        table.add_column("Status")

        for s in sorted(all_stats, key=lambda x: x["company_id"]):
            ok = "✔" in s["status"]
            status_str = (
                f"[green]{s['status']}[/green]" if ok
                else f"[red]{s['status']}[/red]"
            )
            table.add_row(
                s["company_name"],
                f"{s['nodes']:,}",
                f"{s['relationships']:,}",
                f"{s['junction_rels']:,}",
                str(s["duration_s"]),
                status_str,
            )

        console.print(table)
        console.print(
            f"\n[bold]Total time: {total_s:.1f}s | "
            f"Companies: {len(all_stats)}[/bold]"
        )


@click.command()
@click.option("--registry", default="config/tenants.yaml",
              help="Path to tenants.yaml")
@click.option("--company", default=None,
              help="Run for a specific company_id only")
@click.option("--introspect-only", is_flag=True,
              help="Only run schema introspection, skip ETL")
def main(registry, company, introspect_only):
    """
    Multi-tenant ERP → Neo4j migration.
    Runs all companies in parallel with strict isolation.
    """
    pipeline = MultiTenantPipeline(registry, company_filter=company)

    if introspect_only:
        pipeline.run_introspection()
    else:
        pipeline.run()


if __name__ == "__main__":
    main()
