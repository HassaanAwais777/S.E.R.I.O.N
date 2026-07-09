# erp2neo4j

A production-grade pipeline to migrate and sync ERP data from PostgreSQL to Neo4j at scale (50M+ rows).

## Architecture

```
PostgreSQL  ──►  Schema Introspector  ──►  Mapping Engine (YAML)
                                                    │
                                          Data Quality Gate
                                                    │
                                     Parallel ETL Core (chunked)
                                                    │
                                          Write Optimizer
                                                    │
                                           Neo4j Cluster
                                                    │
                                     CDC Sync (pg_logical / WAL)
                                                    │
                                       Reconciliation & Audit
```

## Quickstart

```bash
pip install -r requirements.txt

# 1. Introspect your Postgres schema
python -m erp2neo4j.core.introspector --pg-url "postgresql://user:pass@host/db" --output config/schema.json

# 2. Review & edit the auto-generated mapping
cat config/mapping.yaml

# 3. Run full migration
python -m erp2neo4j.etl.pipeline --mapping config/mapping.yaml --mode full

# 4. Start live CDC sync
python -m erp2neo4j.sync.cdc_sync --mapping config/mapping.yaml
```

## Components

| Module | Purpose |
|---|---|
| `core/introspector.py` | Auto-reads Postgres schema, outputs mapping config |
| `core/mapping_engine.py` | YAML-driven node/relationship rule processor |
| `core/quality_gate.py` | Pre-load data validation & cleansing |
| `etl/pipeline.py` | Parallel chunked ETL orchestrator |
| `etl/extractor.py` | Cursor-based streaming Postgres reader |
| `etl/transformer.py` | Row → Cypher parameter transformer |
| `etl/loader.py` | Batched Neo4j writer with UNWIND |
| `sync/cdc_sync.py` | Lightweight CDC via pg_logical / watermarks |
| `sync/reconciler.py` | Count & checksum reconciliation |
| `utils/neo4j_admin.py` | Index/constraint pre-creation helpers |
| `utils/logger.py` | Structured logging |

## Configuration

Set environment variables or use a `.env` file:

```env
PG_URL=postgresql://user:pass@localhost:5432/erp_db
NEO4J_URI=bolt://localhost:7687
NEO4J_USER=neo4j
NEO4J_PASSWORD=password
BATCH_SIZE=5000
NUM_WORKERS=8
CDC_POLL_INTERVAL=10
```
