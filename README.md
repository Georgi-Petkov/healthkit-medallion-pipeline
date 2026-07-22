# healthkit-medallion-pipeline

End-to-end Bronze/Silver/Gold data pipeline for Apple HealthKit exports —
built as a real, running system (not a diagram), verified end-to-end against
production data (1M+ rows in Silver as of the last full ingestion run).

**Running cost: $0/month.** Originally built on Azure (Databricks + ADLS +
Key Vault + a Function App), then fully rebuilt on Databricks Free Edition
after the Azure setup was torn down over an unexpected ~$37/mo NAT Gateway
charge — see [`INFRA_STATUS.md`](INFRA_STATUS.md) for the full story of both
the teardown and the rebuild.

```
iPhone (Health Auto Export app)
        │  auto-sync, JSON export
        ▼
Google Drive                                    ← replaces the old Azure Function ingest
        │  Databricks Unity Catalog Google Drive connection
        │  + Auto Loader (cloudFiles), scheduled daily
        ▼
BRONZE   workspace.healthkit.bronze_health_export   (Delta, Unity Catalog)
        │  dbt — VARIANT explode (parse_json + variant_explode)
        ▼
SILVER   dbt views: base_healthkit_metrics → stg_healthkit_metrics
        │  dbt (gold marts)
        ▼
GOLD     dbt marts: daily activity summary, weekly trends, metric freshness
```

**Start here:** [`PIPELINE_ARCHITECTURE.md`](PIPELINE_ARCHITECTURE.md) —
a detailed walkthrough of exactly what happens at each layer, what's
validated vs. not, the auth chain, CI/CD, and how the current architecture
differs from the original Azure/Lakeflow design (still documented in full,
since it's the more hardened of the two).

## Layout

- `dbt/healthkit/` — the whole pipeline that's actually live: Bronze→Silver
  VARIANT-explode model, Silver→Gold transforms, tests, docs
- `healthkit-ingest-func/` — **retired**, Azure Function ingestion (Health
  Auto Export → HTTPS POST → ADLS). Replaced by the Google Drive connector;
  kept in the repo as a reference implementation, not currently deployed.
- `databricks/healthkit_pipeline/` — **retired**, Databricks Lakeflow
  Declarative Pipeline (Bronze → Silver with Auto CDC dedup). Replaced by
  the dbt VARIANT-explode model above; kept as a reference implementation,
  not currently deployed. Note: the current dbt Silver model does **not**
  reimplement the Auto CDC dedup this pipeline did — see
  `PIPELINE_ARCHITECTURE.md` for what that trade-off actually means.
- `.github/workflows/` — CI for the (retired) Function App and for dbt.
  The dbt workflow still targets the old Azure OIDC/Key Vault auth chain and
  needs updating for the new Free Edition token auth before it'll pass again.
- The Bronze ingestion notebook itself lives only in the Databricks
  workspace (uploaded directly, not deployed from this repo) — not yet
  pulled into version control here.

## Security notes

No plaintext secrets or storage keys committed anywhere in this repo, in
either architecture. Current setup: a Databricks personal access token,
loaded from a gitignored `.env` (`dbt/healthkit/.env`) via `env_var()` in
`profiles.yml` — nothing in the tracked config resolves to a real secret.
The original Azure setup's auth chain (managed identity + OAuth + Key Vault,
zero long-lived secrets) is documented in full in the "Governance & auth"
section of `PIPELINE_ARCHITECTURE.md`, since it's a stronger pattern than
personal-token auth and worth showing even though it's not what's live now.
