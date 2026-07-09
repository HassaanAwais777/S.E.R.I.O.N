"""
Field Encryptor — PII Protection at Rest
-----------------------------------------
Layer 5: Encrypts sensitive fields (email, phone, address, national ID, etc.)
BEFORE writing to Neo4j. Decrypts on read.

Each company gets its own encryption key — even if someone accesses
another company's Neo4j database, encrypted fields are unreadable
without the correct company key.

Encryption: AES-256-GCM (authenticated encryption)
  - Confidentiality: data unreadable without key
  - Integrity: tamper detection built in
  - Each value gets a unique nonce — same value encrypted twice ≠ same ciphertext

Key storage: Keys are stored in config/keys/{company_id}.key (chmod 600)
             In production: use AWS KMS, HashiCorp Vault, or Azure Key Vault.

Usage:
    enc = FieldEncryptor("company_acme")
    encrypted_row = enc.encrypt_row(row, fields=["email", "phone", "address"])
    decrypted_row = enc.decrypt_row(encrypted_row, fields=["email", "phone", "address"])
"""
import os
import base64
import json
from utils.logger import get_logger

log = get_logger(__name__)

KEYS_DIR = "config/keys"

# Fields that MUST be encrypted for any ERP dataset
DEFAULT_PII_FIELDS = {
    "email", "phone", "mobile", "address", "street",
    "national_id", "passport", "tax_id", "ssn", "nric",
    "bank_account", "iban", "credit_card",
    "date_of_birth", "dob", "salary", "wage",
    "password", "password_hash",
}


def _import_crypto():
    """Lazy import — avoid hard dependency if not using encryption."""
    try:
        from cryptography.hazmat.primitives.ciphers.aead import AESGCM
        from cryptography.hazmat.primitives.kdf.scrypt import Scrypt
        from cryptography.hazmat.backends import default_backend
        import secrets as _secrets
        return AESGCM, _secrets
    except ImportError:
        raise ImportError(
            "Install cryptography: pip install cryptography --break-system-packages"
        )


class FieldEncryptor:
    """
    Per-company AES-256-GCM field encryptor.
    One key file per company, stored at config/keys/{company_id}.key
    """

    ENCRYPTED_PREFIX = "enc::"    # Marks an encrypted field value

    def __init__(self, company_id: str):
        self.company_id = company_id
        self._key: bytes = None
        os.makedirs(KEYS_DIR, exist_ok=True)

    def _key_path(self) -> str:
        return os.path.join(KEYS_DIR, f"{self.company_id}.key")

    def _load_or_generate_key(self) -> bytes:
        if self._key:
            return self._key

        path = self._key_path()
        if os.path.exists(path):
            with open(path, "rb") as f:
                self._key = base64.b64decode(f.read().strip())
            log.debug(f"[{self.company_id}] Encryption key loaded")
        else:
            # Generate new 256-bit key
            AESGCM, secrets = _import_crypto()
            self._key = AESGCM.generate_key(bit_length=256)
            with open(path, "wb") as f:
                f.write(base64.b64encode(self._key))
            os.chmod(path, 0o600)
            log.info(
                f"[{self.company_id}] "
                f"[green]✔ New encryption key generated: {path}[/green]"
            )

        return self._key

    def encrypt_value(self, plaintext: str) -> str:
        """
        Encrypt a single string value.
        Returns: "enc::<base64(nonce+ciphertext)>"
        """
        if plaintext is None:
            return None
        if isinstance(plaintext, str) and plaintext.startswith(self.ENCRYPTED_PREFIX):
            return plaintext   # already encrypted

        AESGCM, secrets = _import_crypto()
        key = self._load_or_generate_key()
        aesgcm = AESGCM(key)
        nonce = secrets.token_bytes(12)    # 96-bit nonce for GCM
        ciphertext = aesgcm.encrypt(nonce, str(plaintext).encode(), None)
        encoded = base64.b64encode(nonce + ciphertext).decode()
        return f"{self.ENCRYPTED_PREFIX}{encoded}"

    def decrypt_value(self, encrypted: str) -> str:
        """Decrypt a value encrypted by encrypt_value."""
        if encrypted is None:
            return None
        if not isinstance(encrypted, str):
            return encrypted
        if not encrypted.startswith(self.ENCRYPTED_PREFIX):
            return encrypted   # not encrypted — return as-is

        AESGCM, _ = _import_crypto()
        key = self._load_or_generate_key()
        aesgcm = AESGCM(key)
        raw = base64.b64decode(encrypted[len(self.ENCRYPTED_PREFIX):])
        nonce = raw[:12]
        ciphertext = raw[12:]
        return aesgcm.decrypt(nonce, ciphertext, None).decode()

    def encrypt_row(self, row: dict, fields: set[str] = None) -> dict:
        """
        Encrypt specified fields in a row dict.
        Uses DEFAULT_PII_FIELDS if fields not specified.
        Fields not in the row are skipped silently.
        """
        target_fields = fields or DEFAULT_PII_FIELDS
        result = dict(row)
        for field in target_fields:
            if field in result and result[field] is not None:
                result[field] = self.encrypt_value(str(result[field]))
        return result

    def decrypt_row(self, row: dict, fields: set[str] = None) -> dict:
        """Decrypt specified fields in a row dict."""
        target_fields = fields or DEFAULT_PII_FIELDS
        result = dict(row)
        for field in target_fields:
            if field in result and result[field] is not None:
                result[field] = self.decrypt_value(result[field])
        return result

    def encrypt_batch(self, rows: list[dict], fields: set[str] = None) -> list[dict]:
        return [self.encrypt_row(row, fields) for row in rows]

    def decrypt_batch(self, rows: list[dict], fields: set[str] = None) -> list[dict]:
        return [self.decrypt_row(row, fields) for row in rows]

    def rotate_key(self, old_rows: list[dict], fields: set[str] = None) -> list[dict]:
        """
        Key rotation: decrypt with old key, generate new key, re-encrypt.
        Old key file is backed up as {company_id}.key.bak
        """
        import shutil
        path = self._key_path()
        if os.path.exists(path):
            shutil.copy2(path, path + ".bak")
            log.info(f"Old key backed up to {path}.bak")

        # Decrypt with old key
        decrypted = self.decrypt_batch(old_rows, fields)

        # Force new key generation
        self._key = None
        os.remove(path)
        self._load_or_generate_key()

        # Re-encrypt with new key
        return self.encrypt_batch(decrypted, fields)

    def get_pii_fields_for_table(self, table: str, columns: list[str]) -> set[str]:
        """
        Auto-detect which columns in a table are PII.
        Intersects DEFAULT_PII_FIELDS with actual column names.
        """
        return DEFAULT_PII_FIELDS.intersection(set(columns))
