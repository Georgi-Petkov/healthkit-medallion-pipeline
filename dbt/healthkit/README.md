# healthkit dbt project

Silver -> Gold transforms for the HealthKit medallion pipeline, targeting the
`healthkit` catalog in Azure Databricks (Unity Catalog).

## Models

- `models/staging/stg_healthkit_metrics.sql` - typed staging view over
  `healthkit.silver.healthkit_metrics` (materialized in schema `silver`).
- `models/marts/fct_daily_activity_summary.sql` - one row per day, key
  activity/vitals metrics pivoted into columns.
- `models/marts/fct_weekly_trends.sql` - week-over-week trend with
  absolute/percent change vs. the prior week.
- `models/marts/fct_metric_freshness.sql` - per-metric coverage and
  staleness, for monitoring pipeline health.

Marts materialize as tables in schema `gold`.

## Auth

Uses a Databricks service principal (`dbt-healthkit-ci`) with OAuth M2M —
no PAT, no plaintext secret in the repo. The client secret lives in the
`healthkit-ingest-kv` Key Vault as `dbt-databricks-client-secret`, same
pattern as the Function App's API key.

Required env vars (see `profiles.yml`):

```bash
export DATABRICKS_HOST="adb-7405605320524740.0.azuredatabricks.net"
export DATABRICKS_HTTP_PATH="/sql/1.0/warehouses/997b45263de388bd"
export DATABRICKS_CLIENT_ID="6b45a84c-46ce-4399-9b6a-44bcae35bf65"
export DATABRICKS_CLIENT_SECRET="$(az keyvault secret show --vault-name healthkit-ingest-kv --name dbt-databricks-client-secret --query value -o tsv)"
```

`DATABRICKS_CLIENT_ID` is a public identifier (like a username), safe to
keep in shell history/CI config - only the secret is sensitive, and that's
fetched from Key Vault at run time, never stored.

## Running locally

```bash
python3 -m venv ../.venv && source ../.venv/bin/activate
pip install dbt-databricks
export DBT_PROFILES_DIR="."
dbt build
dbt source freshness
dbt docs generate && dbt docs serve
```

## Docs / lineage graph

A point-in-time snapshot of `dbt docs generate` output is committed at
`docs_site/index.html` (open directly in a browser - no server needed,
it's a static single-page app reading the sibling `catalog.json` /
`manifest.json`). Regenerate with `dbt docs generate` and copy
`target/{index.html,catalog.json,manifest.json}` into `docs_site/` to
refresh it.

## Ad hoc queries

Two options, both using SQLAlchemy's `databricks` dialect authenticating
via your local `az login` session (`auth_type=azure-cli`) - no token or
secret needed, since these are read-only and just for your own eyeballing.

**One-off from the CLI** - `scripts/query_gold_tail.py` prints the most
recent rows of a gold table:

```bash
pip install -r scripts/requirements.txt
python3 scripts/query_gold_tail.py                                # fct_weekly_trends, 10 rows
python3 scripts/query_gold_tail.py fct_daily_activity_summary 20
python3 scripts/query_gold_tail.py fct_metric_freshness
```

**Iterating on queries** - `scripts/query_gold.ipynb` opens one connection
and gives you a `q("SELECT ...")` helper to re-run with different SQL
across cells without reconnecting each time:

```bash
pip install -r scripts/requirements.txt
python3 -m ipykernel install --user --name healthkit-dbt --display-name "healthkit (dbt venv)"
# then open scripts/query_gold.ipynb and select the "healthkit (dbt venv)" kernel
```

<!-- CI verification commit -->
