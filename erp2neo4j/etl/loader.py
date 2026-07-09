"""
Loader
------
Batched Neo4j writer using UNWIND for maximum throughput.
Always uses MERGE to make loads idempotent (safe to re-run).
Write order: nodes first, then relationships (never reversed).
"""
from neo4j import GraphDatabase
from tenacity import retry, stop_after_attempt, wait_exponential
from core.mapping_engine import NodeDef, RelDef, JunctionRelDef
from utils.logger import get_logger

log = get_logger(__name__)


class Loader:
    def __init__(self, uri: str, user: str, password: str, batch_size: int = 1000):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))
        self.batch_size = batch_size

    def close(self):
        self.driver.close()

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(min=1, max=10))
    def _write_batch(self, cypher: str, rows: list[dict]):
        with self.driver.session() as session:
            session.run(cypher, {"rows": rows})

    def load_nodes(self, node_def: NodeDef, rows: list[dict]) -> int:
        """
        MERGE nodes in batches using UNWIND.
        Returns number of rows written.
        """
        label = node_def.label
        id_field = node_def.id_field
        props = node_def.properties

        # Build SET clause for all properties
        set_clause = ", ".join(f"n.{p} = row.{p}" for p in props)

        cypher = f"""
        UNWIND $rows AS row
        MERGE (n:{label} {{{id_field}: row.{id_field}}})
        {"SET " + set_clause if set_clause else ""}
        """

        written = 0
        for i in range(0, len(rows), self.batch_size):
            batch = rows[i:i + self.batch_size]
            self._write_batch(cypher, batch)
            written += len(batch)

        return written

    def load_relationships(self, rel_def: RelDef,
                           rows: list[dict],
                           from_label: str, to_label: str,
                           from_id_field: str, to_id_field: str) -> int:
        """
        MERGE relationships between already-loaded nodes.
        Both endpoint nodes must exist before calling this.
        """
        rel_type = rel_def.type
        props = rel_def.properties
        set_clause = ", ".join(f"r.{p} = row.{p}" for p in props)

        cypher = f"""
        UNWIND $rows AS row
        MATCH (a:{from_label} {{{from_id_field}: row.from_id}})
        MATCH (b:{to_label}   {{{to_id_field}:  row.to_id}})
        MERGE (a)-[r:{rel_type}]->(b)
        {"SET " + set_clause if set_clause else ""}
        """

        written = 0
        for i in range(0, len(rows), self.batch_size):
            batch = rows[i:i + self.batch_size]
            self._write_batch(cypher, batch)
            written += len(batch)

        return written

    def load_junction_relationships(self, jrel_def: JunctionRelDef,
                                    rows: list[dict],
                                    from_label: str, to_label: str,
                                    from_id_field: str, to_id_field: str) -> int:
        rel_type = jrel_def.type
        props = jrel_def.properties
        set_clause = ", ".join(f"r.{p} = row.{p}" for p in props)

        cypher = f"""
        UNWIND $rows AS row
        MATCH (a:{from_label} {{{from_id_field}: row.from_id}})
        MATCH (b:{to_label}   {{{to_id_field}:  row.to_id}})
        MERGE (a)-[r:{rel_type}]->(b)
        {"SET " + set_clause if set_clause else ""}
        """

        written = 0
        for i in range(0, len(rows), self.batch_size):
            batch = rows[i:i + self.batch_size]
            self._write_batch(cypher, batch)
            written += len(batch)

        return written

    def delete_node(self, label: str, id_field: str, id_value):
        """Used by CDC sync for deleted rows."""
        cypher = f"""
        MATCH (n:{label} {{{id_field}: $id_value}})
        DETACH DELETE n
        """
        with self.driver.session() as session:
            session.run(cypher, {"id_value": id_value})

    def upsert_node(self, node_def: NodeDef, row: dict):
        """Single-row upsert for CDC sync."""
        self.load_nodes(node_def, [row])
