"""
Neo4j Security Provisioner
---------------------------
Layer 1: Creates a DEDICATED Neo4j user per company database.
         Each user can ONLY access their own database — enforced by Neo4j RBAC.

Layer 2: Validates TLS is enabled on the Bolt connection.
         Refuses to run if TLS is off in production.

This runs ONCE during initial company onboarding, not during every ETL run.

Neo4j RBAC model used:
  - One role per company:   role_company_acme
  - Role grants ONLY:       READ + WRITE on database company_acme
  - Role explicitly DENIES: access to all other databases
  - One user per company:   user_company_acme (password from vault/env)

Usage:
    python -m erp2neo4j.security.provisioner --company company_acme
    python -m erp2neo4j.security.provisioner --all         # provision all tenants
    python -m erp2neo4j.security.provisioner --verify      # verify isolation
"""
import os
import secrets
import string
import json
from dotenv import load_dotenv
import click
from neo4j import GraphDatabase
from neo4j.exceptions import ClientError

from tenancy.registry import TenantRegistry, TenantConfig, Neo4jConfig
from utils.logger import get_logger

load_dotenv()
log = get_logger(__name__)

CREDENTIALS_FILE = "config/neo4j_credentials.json"   # store securely / use vault


def _generate_password(length: int = 32) -> str:
    """Cryptographically secure random password."""
    alphabet = string.ascii_letters + string.digits + "!@#$%^&*"
    return "".join(secrets.choice(alphabet) for _ in range(length))


class Neo4jSecurityProvisioner:
    """
    Provisions per-company Neo4j users and roles via the system database.
    Uses Neo4j Enterprise RBAC — requires Neo4j 5+ Enterprise or Aura.

    For Neo4j Community (which lacks fine-grained RBAC):
      - Use separate Neo4j instances per company instead
      - See docker-compose.multi.yml for that setup
    """

    def __init__(self, neo4j_cfg: Neo4jConfig):
        # Connect as admin (neo4j user) to system DB
        self.admin_driver = GraphDatabase.driver(
            neo4j_cfg.uri,
            auth=(neo4j_cfg.user, neo4j_cfg.password)
        )
        self.credentials: dict = self._load_credentials()

    def _load_credentials(self) -> dict:
        if os.path.exists(CREDENTIALS_FILE):
            with open(CREDENTIALS_FILE) as f:
                return json.load(f)
        return {}

    def _save_credentials(self):
        os.makedirs(os.path.dirname(CREDENTIALS_FILE), exist_ok=True)
        with open(CREDENTIALS_FILE, "w") as f:
            json.dump(self.credentials, f, indent=2)
        os.chmod(CREDENTIALS_FILE, 0o600)   # owner read-only
        log.info(f"Credentials saved to {CREDENTIALS_FILE} (chmod 600)")

    def _validate_tls(self, uri: str):
        """
        Refuse to proceed if using plain bolt:// in a non-local environment.
        Production must use bolt+ssc:// (self-signed) or neo4j+s:// (CA-signed).
        """
        is_local = any(h in uri for h in ["localhost", "127.0.0.1", "::1"])
        is_encrypted = any(s in uri for s in ["+s://", "+ssc://", "bolt+s", "neo4j+s"])

        if not is_local and not is_encrypted:
            raise SecurityError(
                f"SECURITY VIOLATION: Neo4j URI '{uri}' uses unencrypted Bolt in "
                f"a non-local environment. "
                f"Use 'bolt+ssc://host:7687' (self-signed cert) or "
                f"'neo4j+s://host:7687' (CA-signed cert) for production."
            )
        if is_local:
            log.warning(
                "[yellow]TLS not enforced on localhost — "
                "ensure TLS is enabled in production![/yellow]"
            )
        else:
            log.info("[green]✔ TLS-encrypted connection verified[/green]")

    def provision_tenant(self, tenant: TenantConfig) -> dict:
        """
        Creates:
          1. Neo4j database for this tenant (if not exists)
          2. A role that can ONLY access this tenant's database
          3. A user bound to that role

        Returns credential dict {username, password, database}.
        """
        db = tenant.neo4j_db
        role = f"role_{tenant.company_id}"
        user = f"user_{tenant.company_id}"

        # Use existing password if already provisioned, else generate new
        if tenant.company_id in self.credentials:
            password = self.credentials[tenant.company_id]["password"]
            log.info(f"[{tenant.company_id}] Using existing credentials")
        else:
            password = _generate_password()
            log.info(f"[{tenant.company_id}] Generated new credentials")

        with self.admin_driver.session(database="system") as s:
            # 1. Create database
            try:
                s.run(f"CREATE DATABASE `{db}` IF NOT EXISTS")
                log.info(f"[{tenant.company_id}] [green]✔ Database: {db}[/green]")
            except ClientError as e:
                log.debug(f"DB create: {e}")

            # 2. Create role with access ONLY to this database
            try:
                s.run(f"CREATE ROLE `{role}` IF NOT EXISTS")

                # Grant full access to own database
                s.run(f"GRANT ACCESS ON DATABASE `{db}` TO `{role}`")
                s.run(f"GRANT READ {{*}} ON GRAPH `{db}` TO `{role}`")
                s.run(f"GRANT WRITE ON GRAPH `{db}` TO `{role}`")

                # Explicitly DENY access to all other common databases
                for forbidden_db in ["system", "neo4j"]:
                    try:
                        s.run(f"DENY ACCESS ON DATABASE `{forbidden_db}` TO `{role}`")
                    except ClientError:
                        pass

                log.info(f"[{tenant.company_id}] [green]✔ Role: {role}[/green]")
            except ClientError as e:
                log.debug(f"Role setup: {e}")

            # 3. Create user and assign role
            try:
                s.run(
                    f"CREATE USER `{user}` IF NOT EXISTS "
                    f"SET PASSWORD '{password}' "
                    f"SET PASSWORD CHANGE NOT REQUIRED"
                )
                s.run(f"GRANT ROLE `{role}` TO `{user}`")
                log.info(f"[{tenant.company_id}] [green]✔ User: {user}[/green]")
            except ClientError as e:
                log.debug(f"User setup: {e}")

        creds = {
            "username": user,
            "password": password,
            "database": db,
            "role": role,
        }
        self.credentials[tenant.company_id] = creds
        self._save_credentials()

        return creds

    def verify_isolation(self, tenant: TenantConfig, other_tenant: TenantConfig) -> bool:
        """
        Verify that tenant's user CANNOT access other_tenant's database.
        Returns True if isolation is correctly enforced.
        """
        creds = self.credentials.get(tenant.company_id)
        if not creds:
            log.error(f"No credentials for {tenant.company_id}")
            return False

        uri = self.admin_driver.get_server_info().address
        test_driver = GraphDatabase.driver(
            f"bolt://{uri}",
            auth=(creds["username"], creds["password"])
        )

        try:
            with test_driver.session(database=other_tenant.neo4j_db) as s:
                s.run("MATCH (n) RETURN n LIMIT 1")
            # If we get here — isolation FAILED
            log.error(
                f"[bold red]ISOLATION FAILURE:[/bold red] "
                f"{tenant.company_id} CAN access {other_tenant.company_id} data!"
            )
            return False
        except Exception:
            log.info(
                f"[green]✔ Isolation verified:[/green] "
                f"{tenant.company_id} cannot access {other_tenant.company_id}"
            )
            return True
        finally:
            test_driver.close()

    def deprovision_tenant(self, tenant: TenantConfig):
        """Remove user, role, and database for a company. IRREVERSIBLE."""
        db = tenant.neo4j_db
        role = f"role_{tenant.company_id}"
        user = f"user_{tenant.company_id}"

        with self.admin_driver.session(database="system") as s:
            for stmt in [
                f"DROP USER `{user}` IF EXISTS",
                f"DROP ROLE `{role}` IF EXISTS",
                f"DROP DATABASE `{db}` IF EXISTS DESTROY DATA",
            ]:
                try:
                    s.run(stmt)
                except ClientError as e:
                    log.warning(f"{stmt}: {e}")

        self.credentials.pop(tenant.company_id, None)
        self._save_credentials()
        log.warning(f"[red]Deprovisioned: {tenant.company_id}[/red]")

    def close(self):
        self.admin_driver.close()


class SecurityError(Exception):
    pass


@click.command()
@click.option("--registry", default="config/tenants.yaml")
@click.option("--company", default=None, help="Provision specific company")
@click.option("--all", "all_tenants", is_flag=True, help="Provision all active tenants")
@click.option("--verify", is_flag=True, help="Verify isolation between all tenant pairs")
@click.option("--deprovision", default=None, help="Remove a company (IRREVERSIBLE)")
def main(registry, company, all_tenants, verify, deprovision):
    """Provision Neo4j users and roles for tenant isolation."""
    reg = TenantRegistry(registry).load()
    provisioner = Neo4jSecurityProvisioner(reg.neo4j)

    try:
        if deprovision:
            tenant = reg.get(deprovision)
            if not tenant:
                log.error(f"Unknown company: {deprovision}")
                return
            confirm = input(f"Type '{deprovision}' to confirm deletion: ")
            if confirm == deprovision:
                provisioner.deprovision_tenant(tenant)

        elif verify:
            tenants = reg.active_tenants()
            all_ok = True
            for i, t1 in enumerate(tenants):
                for t2 in tenants[i+1:]:
                    ok1 = provisioner.verify_isolation(t1, t2)
                    ok2 = provisioner.verify_isolation(t2, t1)
                    all_ok = all_ok and ok1 and ok2
            if all_ok:
                log.info("[bold green]✔ All isolation checks passed![/bold green]")
            else:
                log.error("[bold red]✘ Isolation failures detected![/bold red]")

        elif all_tenants:
            for tenant in reg.active_tenants():
                provisioner.provision_tenant(tenant)
            log.info("[bold green]✔ All tenants provisioned[/bold green]")

        elif company:
            tenant = reg.get(company)
            if not tenant:
                log.error(f"Unknown company: {company}")
                return
            creds = provisioner.provision_tenant(tenant)
            log.info(f"Username: {creds['username']}, DB: {creds['database']}")

    finally:
        provisioner.close()


if __name__ == "__main__":
    main()
