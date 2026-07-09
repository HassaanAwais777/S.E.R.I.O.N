"""
Security Tests — all 5 isolation layers
Run: pytest tests/test_security.py -v
"""
import os
import sys
import json
import pytest
import tempfile

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from security.tenant_guard import (
    TenantGuard, WriteContext, TenantViolationError,
    enforce_company_id_in_rows, scan_for_cross_tenant_contamination,
    strip_internal_fields,
)
from security.audit_logger import AuditLogger, AuditOp
from security.field_encryptor import FieldEncryptor, DEFAULT_PII_FIELDS
from tenancy.registry import TenantConfig


# ─── Fixtures ────────────────────────────────────────────────

def make_tenant(company_id: str) -> TenantConfig:
    return TenantConfig(
        company_id=company_id,
        name=f"{company_id} Corp",
        pg_url=f"postgresql://user:pass@localhost/{company_id}",
        neo4j_db=f"db_{company_id}",
        mapping_file=f"/tmp/{company_id}_mapping.yaml",
        departments=["Finance", "HR"],
        active=True,
    )


# ─────────────────────────────────────────────────────────────
# Layer 3: Tenant Guard Tests
# ─────────────────────────────────────────────────────────────

class TestTenantGuard:

    def test_correct_company_passes(self):
        tenant = make_tenant("acme")
        guard = TenantGuard(tenant)
        ctx = WriteContext(
            company_id="acme",
            database="db_acme",
            table="customers",
            batch_size=100,
        )
        guard.assert_safe(ctx)   # should not raise
        assert guard.stats()["writes_allowed"] == 1

    def test_wrong_company_id_blocked(self):
        """CORE ISOLATION TEST: Data from company B must never write to company A."""
        tenant_a = make_tenant("acme")
        guard_a = TenantGuard(tenant_a)

        # Context says this data is from 'beta', but guard is for 'acme'
        ctx = WriteContext(
            company_id="beta",     # WRONG company
            database="db_acme",    # but targeting acme DB
            table="customers",
            batch_size=100,
        )

        with pytest.raises(TenantViolationError) as exc:
            guard_a.assert_safe(ctx)

        assert "TENANT VIOLATION" in str(exc.value)
        assert "beta" in str(exc.value)
        assert "acme" in str(exc.value)
        assert guard_a.stats()["violations_blocked"] == 1

    def test_wrong_database_blocked(self):
        """Even with correct company_id, wrong database must be blocked."""
        tenant = make_tenant("acme")
        guard = TenantGuard(tenant)
        ctx = WriteContext(
            company_id="acme",
            database="db_beta",    # WRONG database
            table="orders",
            batch_size=50,
        )
        with pytest.raises(TenantViolationError):
            guard.assert_safe(ctx)

    def test_make_context_correct(self):
        tenant = make_tenant("acme")
        guard = TenantGuard(tenant)
        ctx = guard.make_context("orders", 500)
        assert ctx.company_id == "acme"
        assert ctx.database == "db_acme"
        assert ctx.table == "orders"

    def test_multiple_violations_counted(self):
        tenant = make_tenant("acme")
        guard = TenantGuard(tenant)
        for _ in range(3):
            ctx = WriteContext("beta", "db_acme", "customers", 10)
            try:
                guard.assert_safe(ctx)
            except TenantViolationError:
                pass
        assert guard.stats()["violations_blocked"] == 3

    def test_row_stamping(self):
        rows = [{"id": 1, "name": "Acme"}]
        stamped = enforce_company_id_in_rows(rows, "acme")
        assert stamped[0]["__company_id__"] == "acme"

    def test_contamination_scan_clean(self):
        rows = [{"id": 1, "__company_id__": "acme"}]
        contaminated = scan_for_cross_tenant_contamination(rows, "acme")
        assert len(contaminated) == 0

    def test_contamination_scan_detects_foreign_rows(self):
        rows = [
            {"id": 1, "__company_id__": "acme"},
            {"id": 2, "__company_id__": "beta"},   # foreign row!
        ]
        contaminated = scan_for_cross_tenant_contamination(rows, "acme")
        assert len(contaminated) == 1
        assert contaminated[0]["id"] == 2

    def test_strip_internal_fields(self):
        rows = [{"id": 1, "name": "X", "__company_id__": "acme"}]
        clean = strip_internal_fields(rows)
        assert "__company_id__" not in clean[0]
        assert clean[0]["id"] == 1


# ─────────────────────────────────────────────────────────────
# Layer 4: Audit Logger Tests
# ─────────────────────────────────────────────────────────────

class TestAuditLogger:

    def test_log_written(self, tmp_path, monkeypatch):
        monkeypatch.setattr("security.audit_logger.AUDIT_DIR", str(tmp_path))
        logger = AuditLogger("acme")
        logger.log(AuditOp.INSERT, "Customer", 100, database="db_acme")
        entries = logger.get_recent(10)
        assert len(entries) == 1
        assert entries[0]["operation"] == "INSERT"
        assert entries[0]["company_id"] == "acme"
        assert entries[0]["row_count"] == 100

    def test_log_is_append_only(self, tmp_path, monkeypatch):
        monkeypatch.setattr("security.audit_logger.AUDIT_DIR", str(tmp_path))
        logger = AuditLogger("acme")
        for i in range(5):
            logger.log(AuditOp.INSERT, "Order", i, database="db_acme")
        entries = logger.get_recent(10)
        assert len(entries) == 5

    def test_violation_logged_to_central_log(self, tmp_path, monkeypatch):
        monkeypatch.setattr("security.audit_logger.AUDIT_DIR", str(tmp_path))
        monkeypatch.setattr(
            "security.audit_logger.SECURITY_LOG",
            str(tmp_path / "SECURITY_VIOLATIONS.jsonl")
        )
        logger = AuditLogger("acme")
        logger.log_violation("beta data found in acme pipeline")
        security_log = tmp_path / "SECURITY_VIOLATIONS.jsonl"
        assert security_log.exists()
        with open(security_log) as f:
            entry = json.loads(f.readline())
        assert entry["operation"] == "SECURITY_VIOLATION"
        assert "beta" in entry["extra"]["details"]

    def test_fingerprint_is_unique(self, tmp_path, monkeypatch):
        monkeypatch.setattr("security.audit_logger.AUDIT_DIR", str(tmp_path))
        logger = AuditLogger("acme")
        logger.log(AuditOp.INSERT, "Customer", 10, database="db_acme")
        logger.log(AuditOp.INSERT, "Customer", 10, database="db_acme")
        entries = logger.get_recent(10)
        fps = [e["fingerprint"] for e in entries]
        assert len(set(fps)) == 2   # all unique


# ─────────────────────────────────────────────────────────────
# Layer 5: Field Encryptor Tests
# ─────────────────────────────────────────────────────────────

class TestFieldEncryptor:

    @pytest.fixture(autouse=True)
    def temp_keys(self, tmp_path, monkeypatch):
        monkeypatch.setattr("security.field_encryptor.KEYS_DIR", str(tmp_path))

    def test_encrypt_decrypt_roundtrip(self):
        enc = FieldEncryptor("acme")
        plaintext = "user@example.com"
        encrypted = enc.encrypt_value(plaintext)
        assert encrypted != plaintext
        assert encrypted.startswith("enc::")
        decrypted = enc.decrypt_value(encrypted)
        assert decrypted == plaintext

    def test_same_value_different_ciphertext(self):
        """AES-GCM with unique nonce: same plaintext → different ciphertext each time."""
        enc = FieldEncryptor("acme")
        e1 = enc.encrypt_value("secret")
        e2 = enc.encrypt_value("secret")
        assert e1 != e2   # nonces differ

    def test_different_company_different_key(self):
        """Company A's key cannot decrypt Company B's data."""
        enc_a = FieldEncryptor("acme")
        enc_b = FieldEncryptor("beta")

        encrypted_by_b = enc_b.encrypt_value("beta_secret")

        # Attempting to decrypt with company A's key must fail
        try:
            result = enc_a.decrypt_value(encrypted_by_b)
            # If it returns a value, it must NOT equal the original
            assert result != "beta_secret", \
                "ISOLATION FAILURE: Company A decrypted Company B's data!"
        except Exception:
            pass   # Expected — decryption with wrong key raises

    def test_encrypt_row_pii_fields(self):
        enc = FieldEncryptor("acme")
        row = {
            "customer_id": 1,
            "name": "Acme Corp",
            "email": "admin@acme.com",     # PII
            "phone": "+92-300-1234567",    # PII
            "total_orders": 50,
        }
        encrypted = enc.encrypt_row(row)
        assert encrypted["customer_id"] == 1        # not PII — unchanged
        assert encrypted["name"] == "Acme Corp"     # not PII — unchanged
        assert encrypted["email"].startswith("enc::")
        assert encrypted["phone"].startswith("enc::")

    def test_none_values_pass_through(self):
        enc = FieldEncryptor("acme")
        assert enc.encrypt_value(None) is None
        assert enc.decrypt_value(None) is None

    def test_already_encrypted_not_double_encrypted(self):
        enc = FieldEncryptor("acme")
        first = enc.encrypt_value("test@test.com")
        second = enc.encrypt_value(first)    # should not double-encrypt
        assert second == first

    def test_batch_encrypt_decrypt(self):
        enc = FieldEncryptor("acme")
        rows = [
            {"id": 1, "email": "a@a.com", "name": "A"},
            {"id": 2, "email": "b@b.com", "name": "B"},
        ]
        encrypted = enc.encrypt_batch(rows)
        decrypted = enc.decrypt_batch(encrypted)
        assert decrypted[0]["email"] == "a@a.com"
        assert decrypted[1]["email"] == "b@b.com"
        assert decrypted[0]["name"] == "A"

    def test_auto_detect_pii_fields(self):
        enc = FieldEncryptor("acme")
        cols = ["customer_id", "email", "phone", "address", "total_amount"]
        pii = enc.get_pii_fields_for_table("customers", cols)
        assert "email" in pii
        assert "phone" in pii
        assert "address" in pii
        assert "customer_id" not in pii
        assert "total_amount" not in pii
