"""
Tenant Registry
---------------
Loads tenants.yaml and provides validated Tenant objects.
Each tenant = one company with isolated Postgres source + Neo4j database.
"""
import os
import yaml
from dataclasses import dataclass, field
from utils.logger import get_logger

log = get_logger(__name__)

DEFAULT_REGISTRY = "config/tenants.yaml"


@dataclass
class TenantConfig:
    company_id: str          # unique slug e.g. "company_acme"
    name: str                # display name
    pg_url: str              # postgres source URL
    neo4j_db: str            # neo4j database name (isolated)
    mapping_file: str        # path to this tenant's mapping.yaml
    departments: list[str]   # department names for graph scoping
    active: bool = True

    def __post_init__(self):
        # Allow env-var override for secrets: PG_URL_COMPANY_ACME
        env_key = f"PG_URL_{self.company_id.upper()}"
        env_val = os.getenv(env_key)
        if env_val:
            self.pg_url = env_val


@dataclass
class Neo4jConfig:
    uri: str
    user: str
    password: str

    def __post_init__(self):
        self.uri = os.getenv("NEO4J_URI", self.uri)
        self.user = os.getenv("NEO4J_USER", self.user)
        self.password = os.getenv("NEO4J_PASSWORD", self.password)


class TenantRegistry:
    def __init__(self, registry_path: str = DEFAULT_REGISTRY):
        self.registry_path = registry_path
        self.tenants: list[TenantConfig] = []
        self.neo4j: Neo4jConfig = None
        self._by_id: dict[str, TenantConfig] = {}

    def load(self) -> "TenantRegistry":
        with open(self.registry_path) as f:
            raw = yaml.safe_load(f)

        neo4j_raw = raw.get("neo4j", {})
        self.neo4j = Neo4jConfig(
            uri=neo4j_raw.get("uri", "bolt://localhost:7687"),
            user=neo4j_raw.get("user", "neo4j"),
            password=neo4j_raw.get("password", "password"),
        )

        for t in raw.get("tenants", []):
            tenant = TenantConfig(
                company_id=t["company_id"],
                name=t["name"],
                pg_url=t["pg_url"],
                neo4j_db=t["neo4j_db"],
                mapping_file=t["mapping_file"],
                departments=t.get("departments", []),
                active=t.get("active", True),
            )
            self.tenants.append(tenant)
            self._by_id[tenant.company_id] = tenant

        active = [t for t in self.tenants if t.active]
        log.info(
            f"Tenant registry loaded: "
            f"[cyan]{len(self.tenants)} total[/cyan], "
            f"[green]{len(active)} active[/green]"
        )
        return self

    def get(self, company_id: str) -> TenantConfig | None:
        return self._by_id.get(company_id)

    def active_tenants(self) -> list[TenantConfig]:
        return [t for t in self.tenants if t.active]

    def validate(self):
        """Basic validation — check mapping files exist."""
        errors = []
        for t in self.active_tenants():
            if not os.path.exists(t.mapping_file):
                errors.append(
                    f"[{t.company_id}] mapping_file not found: {t.mapping_file}"
                )
        if errors:
            for e in errors:
                log.error(e)
            raise FileNotFoundError(
                f"{len(errors)} tenant mapping file(s) missing. "
                "Run introspector first or check paths in tenants.yaml."
            )
        log.info("[green]✔ All tenant mapping files found[/green]")
