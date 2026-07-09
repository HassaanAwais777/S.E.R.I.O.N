"""
Transformer
-----------
Converts raw Postgres row dicts into Neo4j-ready parameter dicts.
Handles type coercion, field renaming, and custom transform rules
defined in the mapping config.
"""
from datetime import date, datetime, time
from decimal import Decimal
from core.mapping_engine import NodeDef, RelDef, JunctionRelDef
from utils.logger import get_logger

log = get_logger(__name__)


def _coerce_value(v):
    """Convert Python types that Neo4j doesn't natively support."""
    if isinstance(v, Decimal):
        return float(v)
    if isinstance(v, (datetime, date, time)):
        return v.isoformat()
    if isinstance(v, memoryview):
        return bytes(v).hex()
    return v


class NodeTransformer:
    def __init__(self, node_def: NodeDef):
        self.node_def = node_def
        self.all_fields = [node_def.id_field] + node_def.properties
        self.transforms = node_def.transform  # {"field": "upper" | "lower" | "int" | ...}

    def transform_batch(self, rows: list[dict]) -> list[dict]:
        """Convert a batch of raw Postgres rows to Neo4j parameter dicts."""
        result = []
        for row in rows:
            transformed = {}
            for field in self.all_fields:
                v = row.get(field)
                v = _coerce_value(v)

                # Apply custom transforms from mapping config
                rule = self.transforms.get(field)
                if rule == "upper" and isinstance(v, str):
                    v = v.strip().upper()
                elif rule == "lower" and isinstance(v, str):
                    v = v.strip().lower()
                elif rule == "int" and v is not None:
                    try:
                        v = int(v)
                    except (ValueError, TypeError):
                        v = None
                elif rule == "float" and v is not None:
                    try:
                        v = float(v)
                    except (ValueError, TypeError):
                        v = None

                transformed[field] = v
            result.append(transformed)
        return result


class RelTransformer:
    def __init__(self, rel_def: RelDef):
        self.rel_def = rel_def

    def transform_batch(self, rows: list[dict]) -> list[dict]:
        """Convert rows to dicts with from_id, to_id, and rel properties."""
        result = []
        for row in rows:
            entry = {
                "from_id": _coerce_value(row.get(self.rel_def.from_fk)),
                "to_id":   _coerce_value(row.get(self.rel_def.to_pk)),
            }
            for prop in self.rel_def.properties:
                entry[prop] = _coerce_value(row.get(prop))
            if entry["from_id"] is not None and entry["to_id"] is not None:
                result.append(entry)
        return result


class JunctionRelTransformer:
    def __init__(self, jrel_def: JunctionRelDef):
        self.jrel_def = jrel_def

    def transform_batch(self, rows: list[dict]) -> list[dict]:
        result = []
        for row in rows:
            entry = {
                "from_id": _coerce_value(row.get(self.jrel_def.from_fk)),
                "to_id":   _coerce_value(row.get(self.jrel_def.to_fk)),
            }
            for prop in self.jrel_def.properties:
                entry[prop] = _coerce_value(row.get(prop))
            if entry["from_id"] is not None and entry["to_id"] is not None:
                result.append(entry)
        return result
