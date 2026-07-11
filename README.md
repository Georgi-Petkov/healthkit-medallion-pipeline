# healthkit-medallion-pipeline

End-to-end Bronze/Silver/Gold data pipeline for Apple HealthKit exports —
built as a real, running system (not a diagram), verified end-to-end against
production data.

```
iPhone (Health Auto Export app)
        │  POST, JSON, X-Api-Key header
        ▼
Azure Function (healthkit-ingest-func/)         ← validates, writes raw
        │
        ▼
BRONZE   ADLS Gen2, raw JSON, partitioned by ingestion date
        │  Databricks Lakeflow Declarative Pipeline (databricks/)
        ▼
SILVER   Delta table, deduped via Auto CDC (Unity Catalog)
        │  dbt (dbt/)
        ▼
GOLD     dbt marts: daily activity summary, weekly trends, metric freshness
```

**Start here:** [`PIPELINE_ARCHITECTURE.md`](PIPELINE_ARCHITECTURE.md) —
a detailed walkthrough of exactly what happens at each layer, what's
validated vs. not, the dedup mechanism, the SCD Type 2 demo, the auth chain,
and CI/CD.

## Layout

- `healthkit-ingest-func/` — Azure Function (Python v2), managed-identity
  auth to storage, API-key auth on the endpoint, idempotent writes to Bronze
- `databricks/healthkit_pipeline/` — Lakeflow Declarative Pipeline
  (Bronze → Silver), deployed as a Databricks Asset Bundle, plus a scheduled
  Databricks Job that runs the pipeline and `dbt build` daily
- `dbt/healthkit/` — Silver → Gold transforms, tests, docs, and a couple of
  ad hoc query scripts/notebook for exploring the gold tables
- `.github/workflows/` — CI: lint + test + deploy for the Function App,
  `dbt build`/`test` on PRs touching the dbt project — both authenticate to
  Azure via GitHub OIDC federation, zero secrets stored in GitHub

## Security notes

No plaintext secrets or storage keys anywhere in this repo. Every hop uses
managed identity or OAuth; the two real secrets in the system (the ingest
API key and the dbt service principal's client secret) live in Azure Key
Vault, never in code or CI config. See the "Governance & auth" section of
`PIPELINE_ARCHITECTURE.md` for the full chain.
