"""
Tests for multi-tenant components.
Run: pytest tests/test_tenancy.py
"""
import os
import sys
import pytest
import tempfile
import yaml

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from tenancy.registry import TenantRegistry, TenantConfig
from tenancy.tenant_loader import detect_department


# ─── Fixtures ────────────────────────────────────────────────

SAMPLE_REGISTRY = {
    "neo4j": {
        "uri": "bolt://localhost:7687",
        "user": "neo4j",
        "password": "test",
    },
    "tenants": [
        {
            "company_id": "company_acme",
            "name": "Acme Corp",
            "pg_url": "postgresql://user:pass@localhost/acme",
            "neo4j_db": "company_acme",
            "mapping_file": "/tmp/acme_mapping.yaml",
            "departments": ["Finance", "HR", "Sales"],
            "active": True,
        },
        {
            "company_id": "company_beta",
            "name": "Beta Ltd",
            "pg_url": "postgresql://user:pass@localhost/beta",
            "neo4j_db": "company_beta",
            "mapping_file": "/tmp/beta_mapping.yaml",
            "departments": ["Operations", "Logistics"],
            "active": True,
        },
        {
            "company_id": "company_old",
            "name": "Old Co",
            "pg_url": "postgresql://user:pass@localhost/old",
            "neo4j_db": "company_old",
            "mapping_file": "/tmp/old_mapping.yaml",
            "departments": [],
            "active": False,
        },
    ]
}


@pytest.fixture
def registry_file():
    with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
        yaml.dump(SAMPLE_REGISTRY, f)
        return f.name


# ─── TenantRegistry tests ────────────────────────────────────

class TestTenantRegistry:
    def test_load(self, registry_file):
        reg = TenantRegistry(registry_file).load()
        assert len(reg.tenants) == 3

    def test_active_only(self, registry_file):
        reg = TenantRegistry(registry_file).load()
        active = reg.active_tenants()
        assert len(active) == 2
        assert all(t.active for t in active)

    def test_get_by_id(self, registry_file):
        reg = TenantRegistry(registry_file).load()
        t = reg.get("company_acme")
        assert t is not None
        assert t.name == "Acme Corp"

    def test_get_unknown_id(self, registry_file):
        reg = TenantRegistry(registry_file).load()
        assert reg.get("no_such_company") is None

    def test_neo4j_config_loaded(self, registry_file):
        reg = TenantRegistry(registry_file).load()
        assert reg.neo4j.uri == "bolt://localhost:7687"
        assert reg.neo4j.user == "neo4j"

    def test_isolated_databases(self, registry_file):
        """Each tenant must have a DIFFERENT neo4j_db."""
        reg = TenantRegistry(registry_file).load()
        db_names = [t.neo4j_db for t in reg.tenants]
        assert len(db_names) == len(set(db_names)), \
            "Two tenants share the same Neo4j database — strict isolation violated!"

    def test_departments_per_tenant(self, registry_file):
        reg = TenantRegistry(registry_file).load()
        acme = reg.get("company_acme")
        beta = reg.get("company_beta")
        assert "Finance" in acme.departments
        assert "Finance" not in beta.departments  # Beta has no Finance dept


# ─── Department detection tests ──────────────────────────────

class TestDepartmentDetection:
    def test_finance_detected(self):
        assert detect_department("invoices") == "Finance"
        assert detect_department("accounts_payable") == "Finance"
        assert detect_department("payroll_runs") == "Finance"

    def test_hr_detected(self):
        assert detect_department("employees") == "HR"
        assert detect_department("leave_requests") == "HR"

    def test_sales_detected(self):
        assert detect_department("orders") == "Sales"
        assert detect_department("customers") == "Sales"

    def test_procurement_detected(self):
        assert detect_department("suppliers") == "Procurement"
        assert detect_department("purchase_orders") == "Procurement"

    def test_explicit_override(self):
        """Explicit dept in mapping always wins over auto-detection."""
        result = detect_department("invoices", node_def_dept="Engineering")
        assert result == "Engineering"

    def test_valid_dept_filter(self):
        """If company doesn't have the detected dept, fall through to General."""
        result = detect_department(
            "invoices",
            valid_depts=["Engineering", "Sales"]   # no Finance
        )
        assert result == "General"

    def test_unknown_table(self):
        assert detect_department("xyzzy_table") == "General"

    def test_company_isolation_via_dept(self):
        """
        Acme has Finance, Beta does not.
        Same table 'invoices' → Finance for Acme, General for Beta.
        """
        acme_depts = ["Finance", "HR", "Sales"]
        beta_depts = ["Operations", "Logistics"]

        acme_result = detect_department("invoices", valid_depts=acme_depts)
        beta_result  = detect_department("invoices", valid_depts=beta_depts)

        assert acme_result == "Finance"
        assert beta_result == "General"
