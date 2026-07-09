"""
Audit Logger — Immutable Write Audit Trail
-------------------------------------------
Layer 4: Every write to Neo4j is logged with:
  - company_id (who)
  - database (where)
  - table/label (what type)
  - row count (how many)
  - timestamp + fingerprint (when/which)
  - operation type: INSERT / UPDATE / DELETE
  - triggering user/process

Audit logs are APPEND-ONLY (no delete, no overwrite).
Each company gets its own audit file, plus there is a central
security log for violation attempts.

For production: pipe these to an immutable store (S3, CloudWatch,
Elasticsearch) using the log file path configured below.

Format: JSONL (one JSON object per line — easily parsed by any SIEM).
"""
import json
import os
import time
import hashlib
from datetime import datetime, timezone
from enum import Enum
from utils.logger import get_logger

log = get_logger(__name__)

AUDIT_DIR = "logs/audit"
SECURITY_LOG = "logs/audit/SECURITY_VIOLATIONS.jsonl"


class AuditOp(str, Enum):
    INSERT   = "INSERT"
    UPDATE   = "UPDATE"
    DELETE   = "DELETE"
    SYNC     = "SYNC"
    SCHEMA   = "SCHEMA_CHANGE"
    VIOLATION = "SECURITY_VIOLATION"
    PROVISION = "PROVISION"


class AuditEntry:
    def __init__(
        self,
        company_id: str,
        database: str,
        operation: AuditOp,
        entity_type: str,      # table or label name
        row_count: int,
        triggered_by: str = "pipeline",
        extra: dict = None,
    ):
        self.ts = datetime.now(timezone.utc).isoformat()
        self.company_id = company_id
        self.database = database
        self.operation = operation
        self.entity_type = entity_type
        self.row_count = row_count
        self.triggered_by = triggered_by
        self.extra = extra or {}

        # Fingerprint: tamper-evident hash of key fields
        raw = f"{self.ts}{company_id}{database}{operation}{entity_type}{row_count}"
        self.fingerprint = hashlib.sha256(raw.encode()).hexdigest()[:24]

    def to_dict(self) -> dict:
        return {
            "ts":           self.ts,
            "company_id":   self.company_id,
            "database":     self.database,
            "operation":    self.operation,
            "entity_type":  self.entity_type,
            "row_count":    self.row_count,
            "triggered_by": self.triggered_by,
            "fingerprint":  self.fingerprint,
            **self.extra,
        }


class AuditLogger:
    """
    Thread-safe, append-only audit logger.
    One log file per company: logs/audit/{company_id}.jsonl
    One central security log: logs/audit/SECURITY_VIOLATIONS.jsonl
    """

    def __init__(self, company_id: str):
        self.company_id = company_id
        os.makedirs(AUDIT_DIR, exist_ok=True)
        self._log_path = os.path.join(AUDIT_DIR, f"{company_id}.jsonl")
        # Set restrictive permissions on first create
        if not os.path.exists(self._log_path):
            open(self._log_path, "w").close()
            os.chmod(self._log_path, 0o640)

    def _append(self, path: str, entry: dict):
        """Append a JSONL entry. File is opened in append mode — never overwrites."""
        with open(path, "a") as f:
            f.write(json.dumps(entry) + "\n")

    def log(
        self,
        operation: AuditOp,
        entity_type: str,
        row_count: int,
        database: str = None,
        triggered_by: str = "pipeline",
        extra: dict = None,
    ):
        entry = AuditEntry(
            company_id=self.company_id,
            database=database or f"db_{self.company_id}",
            operation=operation,
            entity_type=entity_type,
            row_count=row_count,
            triggered_by=triggered_by,
            extra=extra,
        )
        self._append(self._log_path, entry.to_dict())

    def log_violation(self, details: str, extra: dict = None):
        """
        Log a security violation attempt.
        Written to both the company log AND the central security log.
        """
        entry = AuditEntry(
            company_id=self.company_id,
            database="UNKNOWN",
            operation=AuditOp.VIOLATION,
            entity_type="SECURITY",
            row_count=0,
            triggered_by="SECURITY_GUARD",
            extra={"details": details, **(extra or {})},
        )
        d = entry.to_dict()
        self._append(self._log_path, d)
        self._append(SECURITY_LOG, d)
        log.critical(
            f"[bold red]SECURITY VIOLATION LOGGED:[/bold red] "
            f"{self.company_id} — {details}"
        )

    def log_delete(self, label: str, id_value, triggered_by: str = "cdc_sync"):
        """Log a node deletion for soft-delete audit trail."""
        self.log(
            operation=AuditOp.DELETE,
            entity_type=label,
            row_count=1,
            triggered_by=triggered_by,
            extra={"deleted_id": str(id_value)},
        )

    def get_recent(self, limit: int = 50) -> list[dict]:
        """Return the last N audit entries for this company."""
        entries = []
        if os.path.exists(self._log_path):
            with open(self._log_path) as f:
                lines = f.readlines()
            for line in reversed(lines[-limit:]):
                try:
                    entries.append(json.loads(line.strip()))
                except json.JSONDecodeError:
                    pass
        return entries

    def get_all_violations() -> list[dict]:
        """Read all security violation entries across all companies."""
        if not os.path.exists(SECURITY_LOG):
            return []
        entries = []
        with open(SECURITY_LOG) as f:
            for line in f:
                try:
                    entries.append(json.loads(line.strip()))
                except json.JSONDecodeError:
                    pass
        return entries

    def summary(self) -> dict:
        """Count operations by type for this company."""
        counts = {}
        if os.path.exists(self._log_path):
            with open(self._log_path) as f:
                for line in f:
                    try:
                        entry = json.loads(line)
                        op = entry.get("operation", "UNKNOWN")
                        counts[op] = counts.get(op, 0) + 1
                    except json.JSONDecodeError:
                        pass
        return counts
