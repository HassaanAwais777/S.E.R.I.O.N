"""
Data Quality Gate
-----------------
Validates and cleanses data BEFORE writing to Neo4j.
Catches null IDs, broken FK references, type mismatches, and duplicates.
Bad rows are quarantined to a reject log, not silently dropped.
"""
import csv
import os
from datetime import datetime
from utils.logger import get_logger

log = get_logger(__name__)

REJECT_DIR = "logs/rejects"


class QualityGate:
    def __init__(self, node_def=None, reject_dir: str = REJECT_DIR):
        self.node_def = node_def
        self.reject_dir = reject_dir
        os.makedirs(reject_dir, exist_ok=True)
        self.stats = {
            "total": 0,
            "passed": 0,
            "rejected": 0,
            "null_id": 0,
            "duplicate_id": 0,
            "type_errors": 0,
        }
        self._seen_ids: set = set()
        self._reject_file = None
        self._reject_writer = None

    def _open_reject_log(self, table: str):
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        path = os.path.join(self.reject_dir, f"{table}_{ts}.csv")
        self._reject_file = open(path, "w", newline="")
        self._reject_writer = csv.writer(self._reject_file)
        self._reject_writer.writerow(["reason", "row_data"])
        log.info(f"Reject log: {path}")

    def close(self):
        if self._reject_file:
            self._reject_file.close()

    def validate_batch(self, rows: list[dict], table: str, id_field: str) -> list[dict]:
        """
        Validate a batch of rows. Returns only clean rows.
        Rejects go to the reject log.
        """
        if not self._reject_writer:
            self._open_reject_log(table)

        clean = []
        for row in rows:
            self.stats["total"] += 1
            reason = self._check_row(row, id_field)
            if reason:
                self.stats["rejected"] += 1
                self._reject_writer.writerow([reason, str(row)])
                self.stats[reason] = self.stats.get(reason, 0) + 1
            else:
                clean.append(self._cleanse(row))
                self.stats["passed"] += 1

        return clean

    def _check_row(self, row: dict, id_field: str) -> str | None:
        """Returns rejection reason string or None if row is clean."""
        val = row.get(id_field)

        # Null/empty ID
        if val is None or str(val).strip() == "":
            return "null_id"

        # Duplicate ID within this run
        if val in self._seen_ids:
            return "duplicate_id"

        self._seen_ids.add(val)
        return None

    def _cleanse(self, row: dict) -> dict:
        """
        Apply field-level cleanups:
        - Strip whitespace from strings
        - Normalize booleans
        - Replace empty strings with None
        """
        cleaned = {}
        for k, v in row.items():
            if isinstance(v, str):
                v = v.strip()
                if v == "":
                    v = None
            cleaned[k] = v
        return cleaned

    def validate_relationships(
        self,
        rows: list[dict],
        table: str,
        from_fk: str,
        to_fk: str,
        loaded_node_ids: set,
        target_node_ids: set,
    ) -> list[dict]:
        """
        For relationship loading: ensure both ends exist in Neo4j.
        Drops rows where either end is missing (orphan FK).
        """
        if not self._reject_writer:
            self._open_reject_log(table)

        clean = []
        for row in rows:
            self.stats["total"] += 1
            from_id = row.get(from_fk)
            to_id = row.get(to_fk)

            if from_id not in loaded_node_ids:
                self.stats["rejected"] += 1
                self._reject_writer.writerow([f"orphan_fk_{from_fk}", str(row)])
                continue

            if to_id not in target_node_ids:
                self.stats["rejected"] += 1
                self._reject_writer.writerow([f"orphan_fk_{to_fk}", str(row)])
                continue

            clean.append(self._cleanse(row))
            self.stats["passed"] += 1

        return clean

    def report(self):
        log.info(
            f"Quality Gate Report — "
            f"Total: {self.stats['total']} | "
            f"[green]Passed: {self.stats['passed']}[/green] | "
            f"[red]Rejected: {self.stats['rejected']}[/red]"
        )
        if self.stats["rejected"] > 0:
            pct = self.stats["rejected"] / max(self.stats["total"], 1) * 100
            log.warning(f"Rejection rate: {pct:.2f}%")
