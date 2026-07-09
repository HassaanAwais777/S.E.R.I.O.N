"""
Unit tests for MappingEngine and Transformer.
Run with: pytest tests/
"""
import pytest
import yaml
import tempfile
import os

import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from core.mapping_engine import MappingEngine
from core.quality_gate import QualityGate
from etl.transformer import NodeTransformer, RelTransformer
from decimal import Decimal
from datetime import datetime


SAMPLE_MAPPING = {
    "version": "1.0",
    "source": {"schema": "public"},
    "nodes": [
        {
            "table": "customers",
            "label": "Customer",
            "id_field": "customer_id",
            "properties": ["name", "email"],
            "indexed_properties": ["email"],
            "estimated_rows": 1000,
            "transform": {"name": "upper"},
        },
        {
            "table": "orders",
            "label": "Order",
            "id_field": "order_id",
            "properties": ["status", "total_amount"],
            "estimated_rows": 5000,
        },
    ],
    "relationships": [
        {
            "type": "PLACED_BY",
            "from": {"table": "orders", "fk": "customer_id"},
            "to":   {"table": "customers", "pk": "customer_id"},
            "properties": [],
        }
    ],
    "junction_relationships": [],
}


@pytest.fixture
def mapping_file():
    with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
        yaml.dump(SAMPLE_MAPPING, f)
        return f.name


class TestMappingEngine:
    def test_load(self, mapping_file):
        engine = MappingEngine(mapping_file).load()
        assert len(engine.nodes) == 2
        assert len(engine.relationships) == 1

    def test_node_labels(self, mapping_file):
        engine = MappingEngine(mapping_file).load()
        labels = [n.label for n in engine.nodes]
        assert "Customer" in labels
        assert "Order" in labels

    def test_get_node_def(self, mapping_file):
        engine = MappingEngine(mapping_file).load()
        nd = engine.get_node_def("customers")
        assert nd is not None
        assert nd.id_field == "customer_id"

    def test_sorted_nodes_by_size(self, mapping_file):
        engine = MappingEngine(mapping_file).load()
        sorted_nodes = engine.sorted_nodes_by_size()
        assert sorted_nodes[0].estimated_rows >= sorted_nodes[-1].estimated_rows

    def test_validation_fails_unknown_table(self):
        bad_mapping = dict(SAMPLE_MAPPING)
        bad_mapping["relationships"] = [
            {
                "type": "BAD_REL",
                "from": {"table": "nonexistent", "fk": "id"},
                "to":   {"table": "customers", "pk": "customer_id"},
                "properties": [],
            }
        ]
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            yaml.dump(bad_mapping, f)
            path = f.name

        with pytest.raises(ValueError, match="Mapping validation failed"):
            MappingEngine(path).load()


class TestNodeTransformer:
    def test_basic_transform(self, mapping_file):
        engine = MappingEngine(mapping_file).load()
        nd = engine.get_node_def("customers")
        transformer = NodeTransformer(nd)

        rows = [{"customer_id": 1, "name": "  acme corp  ", "email": "a@b.com"}]
        result = transformer.transform_batch(rows)

        assert result[0]["name"] == "ACME CORP"   # upper transform applied
        assert result[0]["email"] == "a@b.com"

    def test_decimal_coercion(self, mapping_file):
        engine = MappingEngine(mapping_file).load()
        nd = engine.get_node_def("orders")
        transformer = NodeTransformer(nd)

        rows = [{"order_id": 1, "status": "shipped", "total_amount": Decimal("99.99")}]
        result = transformer.transform_batch(rows)
        assert isinstance(result[0]["total_amount"], float)
        assert result[0]["total_amount"] == 99.99

    def test_datetime_coercion(self, mapping_file):
        engine = MappingEngine(mapping_file).load()
        nd = engine.get_node_def("orders")
        transformer = NodeTransformer(nd)

        dt = datetime(2024, 1, 15, 10, 30)
        rows = [{"order_id": 1, "status": "pending", "total_amount": 10.0, "created_at": dt}]
        result = transformer.transform_batch(rows)
        assert isinstance(result[0].get("created_at") or "", str)


class TestQualityGate:
    def test_null_id_rejected(self):
        gate = QualityGate()
        rows = [
            {"customer_id": None, "name": "Bad"},
            {"customer_id": 1,    "name": "Good"},
        ]
        clean = gate.validate_batch(rows, "customers", "customer_id")
        gate.close()
        assert len(clean) == 1
        assert clean[0]["customer_id"] == 1

    def test_duplicate_id_rejected(self):
        gate = QualityGate()
        rows = [
            {"customer_id": 1, "name": "A"},
            {"customer_id": 1, "name": "B"},  # duplicate
        ]
        clean = gate.validate_batch(rows, "customers", "customer_id")
        gate.close()
        assert len(clean) == 1

    def test_whitespace_cleansed(self):
        gate = QualityGate()
        rows = [{"customer_id": 1, "name": "  spaces  ", "email": ""}]
        clean = gate.validate_batch(rows, "customers", "customer_id")
        gate.close()
        assert clean[0]["name"] == "spaces"
        assert clean[0]["email"] is None

    def test_stats_tracking(self):
        gate = QualityGate()
        rows = [
            {"customer_id": 1, "name": "OK"},
            {"customer_id": None, "name": "Bad"},
        ]
        gate.validate_batch(rows, "customers", "customer_id")
        gate.close()
        assert gate.stats["total"] == 2
        assert gate.stats["passed"] == 1
        assert gate.stats["rejected"] == 1
