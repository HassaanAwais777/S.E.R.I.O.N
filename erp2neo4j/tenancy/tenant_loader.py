"""
Department-Aware Loader
-----------------------
Extends the base Loader to tag every node with a :Department label
and a department property, based on which Postgres schema/table
the data comes from.

Department scoping rules (set in mapping.yaml per node):
  - If department is specified in node_def → use it
  - If table name contains a known dept keyword → auto-detect
  - Otherwise → "General"

This means in Neo4j you can query:
  MATCH (n:Employee {department: "Finance"}) RETURN n
  MATCH (n)-[:WORKS_IN]->(d:Department {name: "HR"}) RETURN n, d
"""
from core.mapping_engine import NodeDef, RelDef, JunctionRelDef
from tenancy.neo4j_manager import TenantNeo4jManager
from utils.logger import get_logger

log = get_logger(__name__)

# Keyword → department auto-detection
DEPT_KEYWORDS = {
    "finance":     "Finance",
    "account":     "Finance",
    "invoice":     "Finance",
    "payroll":     "Finance",
    "hr":          "HR",
    "employee":    "HR",
    "leave":       "HR",
    "recruit":     "HR",
    "procurement": "Procurement",
    "purchase":    "Procurement",
    "supplier":    "Procurement",
    "vendor":      "Procurement",
    "inventory":   "Procurement",
    "product":     "Procurement",
    "sales":       "Sales",
    "order":       "Sales",
    "customer":    "Sales",
    "lead":        "Sales",
    "crm":         "Sales",
    "logistics":   "Logistics",
    "shipment":    "Logistics",
    "delivery":    "Logistics",
    "warehouse":   "Logistics",
    "engineering": "Engineering",
    "project":     "Engineering",
    "ticket":      "Engineering",
    "support":     "Support",
    "marketing":   "Marketing",
    "campaign":    "Marketing",
    "operations":  "Operations",
}


def detect_department(table_name: str, node_def_dept: str = None,
                      valid_depts: list[str] = None) -> str:
    """
    Determine which department a table belongs to.
    Priority: explicit mapping > keyword detection > "General"
    """
    if node_def_dept:
        return node_def_dept

    tl = table_name.lower()
    for keyword, dept in DEPT_KEYWORDS.items():
        if keyword in tl:
            # Only return if this company actually has this department
            if valid_depts and dept not in valid_depts:
                continue
            return dept

    return "General"


class TenantLoader:
    """
    Tenant-scoped loader with department tagging.
    Writes to the tenant's isolated Neo4j database.
    """

    def __init__(self, manager: TenantNeo4jManager, batch_size: int = 1000):
        self.manager = manager
        self.batch_size = batch_size
        self.tenant = manager.tenant

    def load_nodes(self, node_def: NodeDef, rows: list[dict],
                   department: str = None) -> int:
        """
        MERGE nodes with department tag.
        Each node gets:
          - Its normal properties
          - A `department` property (e.g. "Finance")
          - A `company_id` property for audit trail (not for filtering —
            filtering is done at DB level)
        """
        label = node_def.label
        id_field = node_def.id_field
        props = node_def.properties
        dept = department or detect_department(
            node_def.table, None, self.tenant.departments
        )

        set_clause = ", ".join(f"n.{p} = row.{p}" for p in props)
        dept_set = f"n.department = '{dept}', n.company_id = '{self.tenant.company_id}'"

        cypher = f"""
        UNWIND $rows AS row
        MERGE (n:{label} {{{id_field}: row.{id_field}}})
        SET {set_clause + ',' if set_clause else ''} {dept_set}
        """

        written = 0
        for i in range(0, len(rows), self.batch_size):
            batch = rows[i:i + self.batch_size]
            self.manager.write_batch(cypher, batch)
            written += len(batch)

        return written

    def load_relationships(self, rel_def: RelDef, rows: list[dict],
                           from_label: str, to_label: str,
                           from_id_field: str, to_id_field: str) -> int:
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
            self.manager.write_batch(cypher, batch)
            written += len(batch)

        return written

    def load_junction_relationships(self, jrel_def: JunctionRelDef,
                                    rows: list[dict],
                                    from_label: str, to_label: str,
                                    from_id_field: str,
                                    to_id_field: str) -> int:
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
            self.manager.write_batch(cypher, batch)
            written += len(batch)

        return written

    def load_department_nodes(self):
        """
        Create Department nodes for this company's departments.
        These become relationship targets:
          (Employee)-[:WORKS_IN]->(Department {name: "Finance"})
        """
        cypher = """
        UNWIND $rows AS row
        MERGE (d:Department {name: row.name})
        SET d.company_id = row.company_id
        """
        rows = [
            {"name": dept, "company_id": self.tenant.company_id}
            for dept in self.tenant.departments
        ]
        self.manager.write_batch(cypher, rows)
        log.info(
            f"[{self.tenant.company_id}] "
            f"[green]✔ Department nodes created: "
            f"{', '.join(self.tenant.departments)}[/green]"
        )
