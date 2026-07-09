"""
Tenant Guard — Runtime Isolation Enforcer
------------------------------------------
Layer 3: Application-level hard guard that BLOCKS any operation
that would write data from company A into company B's database.

This is a second line of defence — even if a bug in the pipeline
incorrectly routes a company's data, this guard catches it before
any write reaches Neo4j.

How it works:
  1. Every write batch is stamped with a company_id at transform time
  2. Before every Neo4j write, TenantGuard checks:
       batch.company_id == session.database_company_id
  3. If they don't match → HARD BLOCK + SecurityAlert raised + logged

No exception. No fallthrough. Mismatch = exception.
"""
import hashlib
import time
from dataclasses import dataclass
from tenancy.registry import TenantConfig
from utils.logger import get_logger

log = get_logger(__name__)


class TenantViolationError(Exception):
    """Raised when data from company A is about to be written to company B."""
    pass


@dataclass
class WriteContext:
    """Carries tenant identity through the write pipeline."""
    company_id: str
    database: str
    table: str
    batch_size: int
    timestamp: float = None

    def __post_init__(self):
        self.timestamp = self.timestamp or time.time()

    def fingerprint(self) -> str:
        """Unique fingerprint for audit logging."""
        raw = f"{self.company_id}:{self.database}:{self.table}:{self.timestamp}"
        return hashlib.sha256(raw.encode()).hexdigest()[:16]


class TenantGuard:
    """
    Wraps the TenantNeo4jManager write operations with a hard company_id check.

    Usage:
        guard = TenantGuard(tenant)
        guard.assert_safe(write_ctx)   # raises TenantViolationError if mismatch
        manager.write_batch(cypher, rows)
    """

    def __init__(self, tenant: TenantConfig):
        self.tenant = tenant
        self.company_id = tenant.company_id
        self.expected_db = tenant.neo4j_db
        self._write_count = 0
        self._violation_count = 0

    def assert_safe(self, ctx: WriteContext):
        """
        Hard check: company_id in write context must match this tenant.
        Also verifies the target database name matches.

        Raises TenantViolationError immediately on any mismatch.
        """
        # Check 1: company_id match
        if ctx.company_id != self.company_id:
            self._violation_count += 1
            msg = (
                f"TENANT VIOLATION BLOCKED: "
                f"Tried to write data from '{ctx.company_id}' "
                f"into '{self.company_id}' database. "
                f"Table: {ctx.table}, Fingerprint: {ctx.fingerprint()}"
            )
            log.critical(f"[bold red]{msg}[/bold red]")
            raise TenantViolationError(msg)

        # Check 2: database name match (belt-and-suspenders)
        if ctx.database != self.expected_db:
            self._violation_count += 1
            msg = (
                f"DATABASE MISMATCH BLOCKED: "
                f"Context says db='{ctx.database}' but "
                f"expected '{self.expected_db}' for {self.company_id}. "
                f"Fingerprint: {ctx.fingerprint()}"
            )
            log.critical(f"[bold red]{msg}[/bold red]")
            raise TenantViolationError(msg)

        self._write_count += 1

    def make_context(self, table: str, batch_size: int) -> WriteContext:
        """Factory: creates a WriteContext pre-stamped with this tenant's identity."""
        return WriteContext(
            company_id=self.company_id,
            database=self.expected_db,
            table=table,
            batch_size=batch_size,
        )

    def stats(self) -> dict:
        return {
            "company_id": self.company_id,
            "writes_allowed": self._write_count,
            "violations_blocked": self._violation_count,
        }


def enforce_company_id_in_rows(rows: list[dict], company_id: str) -> list[dict]:
    """
    Stamps every row with company_id before transformation.
    This means even if a row somehow crosses pipelines,
    its identity is embedded and can be checked at write time.
    """
    for row in rows:
        row["__company_id__"] = company_id
    return rows


def strip_internal_fields(rows: list[dict]) -> list[dict]:
    """Remove internal pipeline fields before writing to Neo4j."""
    internal = {"__company_id__"}
    return [{k: v for k, v in row.items() if k not in internal} for row in rows]


def scan_for_cross_tenant_contamination(
    rows: list[dict],
    expected_company_id: str
) -> list[dict]:
    """
    Scans a batch for rows that don't belong to this company.
    Returns the contaminated rows (for logging/quarantine).
    Used as a final sweep before any write.
    """
    contaminated = [
        row for row in rows
        if row.get("__company_id__") and row["__company_id__"] != expected_company_id
    ]
    if contaminated:
        log.critical(
            f"[bold red]CONTAMINATION DETECTED:[/bold red] "
            f"{len(contaminated)} rows from a foreign company found in "
            f"{expected_company_id} pipeline. Quarantining."
        )
    return contaminated
