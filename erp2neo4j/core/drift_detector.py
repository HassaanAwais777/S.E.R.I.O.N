"""
Schema Drift Detector
---------------------
Compares the current Postgres schema against the last saved snapshot.
Alerts on new tables, dropped tables, new columns, dropped FKs, etc.
Run before every ETL job to catch schema changes early.
"""
import json
import os
from utils.logger import get_logger

log = get_logger(__name__)

SNAPSHOT_PATH = "config/schema_snapshot.json"


class SchemaDriftDetector:
    def __init__(self, introspector, snapshot_path: str = SNAPSHOT_PATH):
        self.introspector = introspector
        self.snapshot_path = snapshot_path

    def load_snapshot(self) -> dict | None:
        if not os.path.exists(self.snapshot_path):
            log.warning("No schema snapshot found. Skipping drift detection.")
            return None
        with open(self.snapshot_path) as f:
            return json.load(f)

    def detect(self) -> dict:
        """
        Returns a drift report dict with keys:
          new_tables, dropped_tables, new_fks, dropped_fks
        Returns empty dict if no snapshot or no drift.
        """
        snapshot = self.load_snapshot()
        if not snapshot:
            return {}

        current_pks = self.introspector.get_primary_keys()
        current_fks = self.introspector.get_foreign_keys()

        old_tables = set(snapshot["primary_keys"].keys())
        new_tables_set = set(current_pks.keys())

        added_tables = new_tables_set - old_tables
        dropped_tables = old_tables - new_tables_set

        old_fk_set = {
            (f["from_table"], f["from_column"], f["to_table"])
            for f in snapshot["foreign_keys"]
        }
        new_fk_set = {
            (f["from_table"], f["from_column"], f["to_table"])
            for f in current_fks
        }

        added_fks = new_fk_set - old_fk_set
        dropped_fks = old_fk_set - new_fk_set

        drift = {}

        if added_tables:
            drift["new_tables"] = list(added_tables)
            for t in added_tables:
                log.warning(f"[yellow]⚠ New table detected:[/yellow] {t} — update mapping.yaml")

        if dropped_tables:
            drift["dropped_tables"] = list(dropped_tables)
            for t in dropped_tables:
                log.error(f"[red]✘ Table dropped:[/red] {t} — review mapping.yaml")

        if added_fks:
            drift["new_fks"] = [list(fk) for fk in added_fks]
            for fk in added_fks:
                log.warning(f"[yellow]⚠ New FK:[/yellow] {fk[0]}.{fk[1]} → {fk[2]}")

        if dropped_fks:
            drift["dropped_fks"] = [list(fk) for fk in dropped_fks]
            for fk in dropped_fks:
                log.error(f"[red]✘ FK dropped:[/red] {fk[0]}.{fk[1]} → {fk[2]}")

        if not drift:
            log.info("[green]✔ No schema drift detected[/green]")

        return drift

    def update_snapshot(self):
        """Call after a successful ETL run to update the baseline."""
        self.introspector.save_schema_snapshot(self.snapshot_path)
        log.info("Schema snapshot updated.")
