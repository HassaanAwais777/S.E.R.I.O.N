"""
Mapping Engine
--------------
Loads the YAML mapping config and provides validated, structured access
to node definitions, relationship definitions, and transformation rules.
"""
import yaml
from dataclasses import dataclass, field
from typing import Optional
from utils.logger import get_logger

log = get_logger(__name__)


@dataclass
class NodeDef:
    table: str
    label: str
    id_field: str
    properties: list[str]
    indexed_properties: list[str] = field(default_factory=list)
    estimated_rows: int = 0
    transform: dict = field(default_factory=dict)   # optional field transforms


@dataclass
class RelDef:
    type: str
    from_table: str
    from_fk: str
    to_table: str
    to_pk: str
    properties: list[str] = field(default_factory=list)


@dataclass
class JunctionRelDef:
    type: str
    junction_table: str
    from_table: str
    from_fk: str
    to_table: str
    to_fk: str
    properties: list[str] = field(default_factory=list)


class MappingEngine:
    def __init__(self, mapping_path: str):
        self.mapping_path = mapping_path
        self.raw: dict = {}
        self.nodes: list[NodeDef] = []
        self.relationships: list[RelDef] = []
        self.junction_relationships: list[JunctionRelDef] = []
        self._node_by_table: dict[str, NodeDef] = {}

    def load(self) -> "MappingEngine":
        with open(self.mapping_path) as f:
            self.raw = yaml.safe_load(f)

        self._parse_nodes()
        self._parse_relationships()
        self._parse_junction_relationships()
        self._validate()

        log.info(
            f"Mapping loaded: "
            f"[cyan]{len(self.nodes)} nodes[/cyan], "
            f"[cyan]{len(self.relationships)} rels[/cyan], "
            f"[cyan]{len(self.junction_relationships)} junction rels[/cyan]"
        )
        return self

    def _parse_nodes(self):
        for n in self.raw.get("nodes", []):
            nd = NodeDef(
                table=n["table"],
                label=n["label"],
                id_field=n["id_field"],
                properties=n.get("properties", []),
                indexed_properties=n.get("indexed_properties", []),
                estimated_rows=n.get("estimated_rows", 0),
                transform=n.get("transform", {}),
            )
            self.nodes.append(nd)
            self._node_by_table[nd.table] = nd

    def _parse_relationships(self):
        for r in self.raw.get("relationships", []):
            self.relationships.append(RelDef(
                type=r["type"],
                from_table=r["from"]["table"],
                from_fk=r["from"]["fk"],
                to_table=r["to"]["table"],
                to_pk=r["to"]["pk"],
                properties=r.get("properties", []),
            ))

    def _parse_junction_relationships(self):
        for jr in self.raw.get("junction_relationships", []):
            self.junction_relationships.append(JunctionRelDef(
                type=jr["type"],
                junction_table=jr["junction_table"],
                from_table=jr["from"]["table"],
                from_fk=jr["from"]["fk"],
                to_table=jr["to"]["table"],
                to_fk=jr["to"]["fk"],
                properties=jr.get("properties", []),
            ))

    def _validate(self):
        errors = []
        table_names = {n.table for n in self.nodes}

        for r in self.relationships:
            if r.from_table not in table_names:
                errors.append(f"Relationship {r.type}: unknown from_table '{r.from_table}'")
            if r.to_table not in table_names:
                errors.append(f"Relationship {r.type}: unknown to_table '{r.to_table}'")

        for jr in self.junction_relationships:
            if jr.from_table not in table_names:
                errors.append(f"Junction rel {jr.type}: unknown from_table '{jr.from_table}'")
            if jr.to_table not in table_names:
                errors.append(f"Junction rel {jr.type}: unknown to_table '{jr.to_table}'")

        if errors:
            for e in errors:
                log.error(e)
            raise ValueError(f"Mapping validation failed with {len(errors)} errors. Check logs.")

        log.info("[green]✔ Mapping validation passed[/green]")

    def get_node_def(self, table: str) -> Optional[NodeDef]:
        return self._node_by_table.get(table)

    def get_all_tables(self) -> list[str]:
        return [n.table for n in self.nodes]

    def get_junction_tables(self) -> list[str]:
        return [jr.junction_table for jr in self.junction_relationships]

    def sorted_nodes_by_size(self) -> list[NodeDef]:
        """Return nodes sorted largest-first for prioritized loading."""
        return sorted(self.nodes, key=lambda n: n.estimated_rows, reverse=True)
