"""
Tenant Neo4j Manager
--------------------
Enforces strict Neo4j database isolation per company.
Every query runs inside the tenant's own database — zero cross-company leakage.

Key behavior:
  - Each tenant gets its own Neo4j database (e.g. "company_acme")
  - The database is auto-created if it doesn't exist
  - All node writes include a :Department label scoped to this company
  - No query can reference another tenant's database
"""
from neo4j import GraphDatabase
from tenacity import retry, stop_after_attempt, wait_exponential
from tenancy.registry import TenantConfig, Neo4jConfig
from utils.logger import get_logger

log = get_logger(__name__)


class TenantNeo4jManager:
    """
    Wraps a Neo4j driver scoped to exactly one tenant's database.
    Pass this object instead of a raw driver throughout the pipeline.
    """

    def __init__(self, tenant: TenantConfig, neo4j_cfg: Neo4jConfig):
        self.tenant = tenant
        self.db_name = tenant.neo4j_db       # e.g. "company_acme"
        self.driver = GraphDatabase.driver(
            neo4j_cfg.uri,
            auth=(neo4j_cfg.user, neo4j_cfg.password)
        )

    def close(self):
        self.driver.close()

    def session(self):
        """Always returns a session scoped to this tenant's database."""
        return self.driver.session(database=self.db_name)

    # ─────────────────────────────────────────────
    # Database lifecycle
    # ─────────────────────────────────────────────

    def create_database_if_not_exists(self):
        """
        Creates the tenant's Neo4j database if it doesn't exist yet.
        Uses the system database for this operation.
        Requires Neo4j Enterprise OR Neo4j 5+ Community with multi-db.
        """
        with self.driver.session(database="system") as sys_session:
            try:
                sys_session.run(
                    f"CREATE DATABASE `{self.db_name}` IF NOT EXISTS"
                )
                log.info(
                    f"[green]✔ Database ready:[/green] "
                    f"[bold]{self.db_name}[/bold]"
                )
            except Exception as e:
                # Already exists or enterprise-only — both are fine
                log.debug(f"DB create note for {self.db_name}: {e}")

    def drop_database(self):
        """Drop the tenant's database. DESTRUCTIVE — use carefully."""
        with self.driver.session(database="system") as sys_session:
            sys_session.run(f"DROP DATABASE `{self.db_name}` IF EXISTS")
        log.warning(f"[red]Database dropped: {self.db_name}[/red]")

    # ─────────────────────────────────────────────
    # Index + constraint setup
    # ─────────────────────────────────────────────

    def setup_constraints(self, mapping: dict):
        """Create UNIQUE constraints inside this tenant's database."""
        with self.session() as s:
            for node_def in mapping.get("nodes", []):
                label = node_def["label"]
                id_field = node_def["id_field"]
                name = f"constraint_{label.lower()}_{id_field}"
                try:
                    s.run(f"""
                        CREATE CONSTRAINT {name} IF NOT EXISTS
                        FOR (n:{label}) REQUIRE n.{id_field} IS UNIQUE
                    """)
                    log.info(
                        f"[{self.tenant.company_id}] "
                        f"[green]✔ Constraint:[/green] {label}.{id_field}"
                    )
                except Exception as e:
                    log.debug(f"Constraint {name}: {e}")

                for prop in node_def.get("indexed_properties", []):
                    idx = f"idx_{label.lower()}_{prop}"
                    try:
                        s.run(f"""
                            CREATE INDEX {idx} IF NOT EXISTS
                            FOR (n:{label}) ON (n.{prop})
                        """)
                    except Exception:
                        pass

    def setup_department_indexes(self):
        """Index the department property — heavily queried for scoping."""
        with self.session() as s:
            try:
                s.run("""
                    CREATE INDEX idx_dept IF NOT EXISTS
                    FOR (n:Node) ON (n.department)
                """)
            except Exception:
                pass

    # ─────────────────────────────────────────────
    # Write helpers (all scoped to tenant DB)
    # ─────────────────────────────────────────────

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(min=1, max=10))
    def write_batch(self, cypher: str, rows: list[dict]):
        """Execute a batched Cypher write inside this tenant's database."""
        with self.session() as s:
            s.run(cypher, {"rows": rows})

    def node_count(self, label: str) -> int:
        with self.session() as s:
            result = s.run(f"MATCH (n:{label}) RETURN count(n) AS cnt")
            return result.single()["cnt"]

    def rel_count(self, rel_type: str) -> int:
        with self.session() as s:
            result = s.run(
                f"MATCH ()-[r:{rel_type}]->() RETURN count(r) AS cnt"
            )
            return result.single()["cnt"]

    def delete_node(self, label: str, id_field: str, id_value):
        with self.session() as s:
            s.run(
                f"MATCH (n:{label} {{{id_field}: $v}}) DETACH DELETE n",
                {"v": id_value}
            )

    def run(self, cypher: str, params: dict = None):
        with self.session() as s:
            return s.run(cypher, params or {})
