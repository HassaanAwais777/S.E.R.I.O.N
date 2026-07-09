"""
Neo4j Admin Utilities
Pre-creates indexes and constraints before bulk loading.
Always create these BEFORE loading any data for maximum performance.
"""
from neo4j import GraphDatabase
from utils.logger import get_logger

log = get_logger(__name__)


class Neo4jAdmin:
    def __init__(self, uri: str, user: str, password: str):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self.driver.close()

    def setup_constraints_and_indexes(self, mapping: dict):
        """
        Auto-creates UNIQUE constraints and indexes for all node labels
        defined in the mapping config. Must be called before ETL starts.
        """
        with self.driver.session() as session:
            for node_def in mapping.get("nodes", []):
                label = node_def["label"]
                id_field = node_def["id_field"]

                # UNIQUE constraint (also creates index automatically)
                constraint_name = f"constraint_{label.lower()}_{id_field}"
                try:
                    session.run(f"""
                        CREATE CONSTRAINT {constraint_name} IF NOT EXISTS
                        FOR (n:{label}) REQUIRE n.{id_field} IS UNIQUE
                    """)
                    log.info(f"[green]✔ Constraint created:[/green] {label}.{id_field}")
                except Exception as e:
                    log.warning(f"Constraint {constraint_name} skipped: {e}")

                # Additional indexes on commonly filtered properties
                for prop in node_def.get("indexed_properties", []):
                    idx_name = f"idx_{label.lower()}_{prop}"
                    try:
                        session.run(f"""
                            CREATE INDEX {idx_name} IF NOT EXISTS
                            FOR (n:{label}) ON (n.{prop})
                        """)
                        log.info(f"[green]✔ Index created:[/green] {label}.{prop}")
                    except Exception as e:
                        log.warning(f"Index {idx_name} skipped: {e}")

    def drop_all_constraints(self, mapping: dict):
        """Drop all constraints (useful for re-runs during development)."""
        with self.driver.session() as session:
            for node_def in mapping.get("nodes", []):
                label = node_def["label"]
                id_field = node_def["id_field"]
                name = f"constraint_{label.lower()}_{id_field}"
                try:
                    session.run(f"DROP CONSTRAINT {name} IF EXISTS")
                    log.info(f"Dropped constraint: {name}")
                except Exception as e:
                    log.warning(f"Could not drop {name}: {e}")

    def get_node_count(self, label: str) -> int:
        with self.driver.session() as session:
            result = session.run(f"MATCH (n:{label}) RETURN count(n) AS cnt")
            return result.single()["cnt"]

    def get_relationship_count(self, rel_type: str) -> int:
        with self.driver.session() as session:
            result = session.run(f"MATCH ()-[r:{rel_type}]->() RETURN count(r) AS cnt")
            return result.single()["cnt"]

    def run_cypher(self, cypher: str, params: dict = None):
        with self.driver.session() as session:
            return session.run(cypher, params or {})
