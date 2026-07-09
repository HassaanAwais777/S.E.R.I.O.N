"""
ETL Pipeline
------------
Orchestrates the full migration from PostgreSQL to Neo4j.
Uses parallel workers for chunked table loading at 50M+ row scale.

Load order (CRITICAL):
  1. Create Neo4j indexes & constraints
  2. Load ALL nodes (parallel)
  3. Load ALL relationships (after nodes complete)
  4. Load ALL junction relationships
  5. Reconcile counts

Usage:
    python -m erp2neo4j.etl.pipeline --mapping config/mapping.yaml --mode full
"""
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dotenv import load_dotenv
import click
from rich.progress import Progress, SpinnerColumn, BarColumn, TextColumn, TimeElapsedColumn
from rich.console import Console
from rich.table import Table

from core.mapping_engine import MappingEngine, NodeDef
from core.quality_gate import QualityGate
from etl.extractor import Extractor
from etl.transformer import NodeTransformer, RelTransformer, JunctionRelTransformer
from etl.loader import Loader
from utils.neo4j_admin import Neo4jAdmin
from utils.logger import get_logger

load_dotenv()
log = get_logger(__name__)
console = Console()

CHUNK_SIZE = int(os.getenv("CHUNK_SIZE", 100_000))
BATCH_SIZE = int(os.getenv("BATCH_SIZE", 5000))
NUM_WORKERS = int(os.getenv("NUM_WORKERS", 8))


class ETLPipeline:
    def __init__(self, mapping_path: str):
        self.mapping = MappingEngine(mapping_path).load()
        self.pg_url = os.getenv("PG_URL")
        self.neo4j_uri = os.getenv("NEO4J_URI", "bolt://localhost:7687")
        self.neo4j_user = os.getenv("NEO4J_USER", "neo4j")
        self.neo4j_pass = os.getenv("NEO4J_PASSWORD", "password")
        self.stats = {}

    def _make_extractor(self) -> Extractor:
        e = Extractor(self.pg_url, batch_size=BATCH_SIZE)
        e.connect()
        return e

    def _make_loader(self) -> Loader:
        return Loader(self.neo4j_uri, self.neo4j_user, self.neo4j_pass, batch_size=BATCH_SIZE)

    # ─────────────────────────────────────────────
    # PHASE 1: Setup indexes & constraints
    # ─────────────────────────────────────────────
    def setup_neo4j(self):
        log.info("[bold]Phase 1: Creating Neo4j indexes & constraints...[/bold]")
        admin = Neo4jAdmin(self.neo4j_uri, self.neo4j_user, self.neo4j_pass)
        try:
            admin.setup_constraints_and_indexes(self.mapping.raw)
        finally:
            admin.close()

    # ─────────────────────────────────────────────
    # PHASE 2: Load nodes (parallel)
    # ─────────────────────────────────────────────
    def load_all_nodes(self, progress: Progress):
        log.info(f"[bold]Phase 2: Loading nodes ({NUM_WORKERS} workers)...[/bold]")

        def load_node_chunk(node_def: NodeDef, pk_min, pk_max):
            extractor = self._make_extractor()
            loader = self._make_loader()
            gate = QualityGate()
            transformer = NodeTransformer(node_def)
            written = 0
            try:
                all_fields = [node_def.id_field] + node_def.properties
                for batch in extractor.stream_chunk(
                    node_def.table, node_def.id_field,
                    pk_min, pk_max, columns=all_fields
                ):
                    clean = gate.validate_batch(batch, node_def.table, node_def.id_field)
                    transformed = transformer.transform_batch(clean)
                    written += loader.load_nodes(node_def, transformed)
            finally:
                extractor.close()
                loader.close()
                gate.close()
            return written

        total_written = 0
        with ThreadPoolExecutor(max_workers=NUM_WORKERS) as pool:
            futures = {}
            extractor = self._make_extractor()
            try:
                for node_def in self.mapping.nodes:
                    chunks = extractor.get_pk_chunks(
                        node_def.table, node_def.id_field, CHUNK_SIZE
                    )
                    task = progress.add_task(
                        f"[cyan]{node_def.label}[/cyan]", total=len(chunks)
                    )
                    for chunk in chunks:
                        f = pool.submit(load_node_chunk, node_def, chunk[0], chunk[1])
                        futures[f] = (node_def.label, task)
            finally:
                extractor.close()

            for future in as_completed(futures):
                label, task = futures[future]
                try:
                    n = future.result()
                    total_written += n
                    progress.advance(task)
                except Exception as e:
                    log.error(f"Error loading {label}: {e}")

        self.stats["nodes_written"] = total_written
        log.info(f"[green]✔ Nodes loaded: {total_written:,}[/green]")

    # ─────────────────────────────────────────────
    # PHASE 3: Load direct FK relationships
    # ─────────────────────────────────────────────
    def load_all_relationships(self, progress: Progress):
        log.info("[bold]Phase 3: Loading relationships...[/bold]")
        extractor = self._make_extractor()
        loader = self._make_loader()
        total_written = 0

        try:
            for rel_def in self.mapping.relationships:
                from_node = self.mapping.get_node_def(rel_def.from_table)
                to_node = self.mapping.get_node_def(rel_def.to_table)
                if not from_node or not to_node:
                    log.warning(f"Skipping rel {rel_def.type}: missing node definition")
                    continue

                transformer = RelTransformer(rel_def)
                task = progress.add_task(
                    f"[yellow]{rel_def.type}[/yellow]", total=None
                )
                cols = [rel_def.from_fk] + rel_def.properties

                for batch in extractor.stream_table(rel_def.from_table, columns=cols):
                    transformed = transformer.transform_batch(batch)
                    written = loader.load_relationships(
                        rel_def, transformed,
                        from_node.label, to_node.label,
                        from_node.id_field, to_node.id_field
                    )
                    total_written += written
                    progress.advance(task, written)

        finally:
            extractor.close()
            loader.close()

        self.stats["rels_written"] = total_written
        log.info(f"[green]✔ Relationships loaded: {total_written:,}[/green]")

    # ─────────────────────────────────────────────
    # PHASE 4: Load junction relationships
    # ─────────────────────────────────────────────
    def load_junction_relationships(self, progress: Progress):
        log.info("[bold]Phase 4: Loading junction relationships...[/bold]")
        extractor = self._make_extractor()
        loader = self._make_loader()
        total_written = 0

        try:
            for jrel in self.mapping.junction_relationships:
                from_node = self.mapping.get_node_def(jrel.from_table)
                to_node = self.mapping.get_node_def(jrel.to_table)
                if not from_node or not to_node:
                    continue

                transformer = JunctionRelTransformer(jrel)
                task = progress.add_task(
                    f"[magenta]{jrel.type}[/magenta]", total=None
                )
                cols = [jrel.from_fk, jrel.to_fk] + jrel.properties

                for batch in extractor.stream_table(jrel.junction_table, columns=cols):
                    transformed = transformer.transform_batch(batch)
                    written = loader.load_junction_relationships(
                        jrel, transformed,
                        from_node.label, to_node.label,
                        from_node.id_field, to_node.id_field
                    )
                    total_written += written
                    progress.advance(task, written)

        finally:
            extractor.close()
            loader.close()

        self.stats["junction_rels_written"] = total_written
        log.info(f"[green]✔ Junction relationships loaded: {total_written:,}[/green]")

    def print_summary(self):
        table = Table(title="ETL Summary", style="cyan")
        table.add_column("Metric", style="bold")
        table.add_column("Value", justify="right")
        for k, v in self.stats.items():
            table.add_row(k.replace("_", " ").title(), f"{v:,}")
        console.print(table)

    def run(self):
        start = time.time()
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TextColumn("{task.completed}/{task.total}"),
            TimeElapsedColumn(),
            console=console,
        ) as progress:
            self.setup_neo4j()
            self.load_all_nodes(progress)
            self.load_all_relationships(progress)
            self.load_junction_relationships(progress)

        elapsed = time.time() - start
        self.stats["total_time_seconds"] = round(elapsed, 1)
        self.print_summary()
        log.info(f"[bold green]✔ Full migration complete in {elapsed:.1f}s[/bold green]")


@click.command()
@click.option("--mapping", default="config/mapping.yaml", help="Path to mapping.yaml")
@click.option("--mode", default="full", type=click.Choice(["full"]), help="Migration mode")
def main(mapping, mode):
    """Run the ETL pipeline: PostgreSQL → Neo4j."""
    pipeline = ETLPipeline(mapping)
    pipeline.run()


if __name__ == "__main__":
    main()
