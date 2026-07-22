# HealthKit Medallion Pipeline — Architecture & Interview Reference

End-to-end pipeline that takes Apple HealthKit exports from the "Health Auto
Export" iOS app and turns them into queryable analytics tables, following the
Bronze / Silver / Gold ("medallion") pattern.

This project has been built **twice**: first on Azure (hardened, managed
identities throughout, Lakeflow Declarative Pipeline for Bronze→Silver), then
rebuilt on Databricks Free Edition after the Azure setup was torn down over
an unexpected recurring cost (see [`INFRA_STATUS.md`](INFRA_STATUS.md)).
Both designs are documented here — the current section below reflects what's
actually live today; the original Azure/Lakeflow design is kept further down
since it's the more hardened of the two and still worth showing.

---

## Current architecture (rebuilt 2026-07-22, $0/month)

```
iPhone (Health Auto Export app)
        │  auto-sync, JSON export
        ▼
Google Drive                                    ← replaces the Azure Function
        │  Databricks Unity Catalog Google Drive connection
        │  (`autohealthexport`, Beta connector, OAuth_U2M)
        │  + Auto Loader (cloudFiles), scheduled daily 05:00 Europe/Copenhagen
        ▼
BRONZE   workspace.healthkit.bronze_health_export   (Delta, Unity Catalog)
        │  dbt: parse_json + variant_explode + dedup (base_healthkit_metrics.sql)
        ▼
SILVER   dbt views: base_healthkit_metrics → stg_healthkit_metrics
        │  dbt (gold marts, unchanged from the original design)
        │  scheduled daily 06:00 Europe/Copenhagen
        ▼
GOLD     fct_daily_activity_summary / fct_weekly_trends / fct_metric_freshness
```

**Why it changed:** Databricks Free Edition has no classic (all-purpose)
clusters — notebook compute is serverless-only, and the Lakeflow Declarative
Pipeline API used in the original design isn't available the same way on
serverless. Rather than block on that, Bronze→Silver was rebuilt in dbt
using Databricks' **VARIANT type** (`parse_json`, `variant_explode`) instead
of a rigid `from_json` schema — necessary because HealthKit metric shapes
are genuinely heterogeneous (most readings are `{source, date, qty}`, but
`sleep_analysis` has no `qty` at all, `core`/`rem`/`deep`/`awake` instead).
Verified end-to-end at real scale: **1M+ rows** through `base_healthkit_metrics`,
all 25 dbt tests passing.

**Ingestion trigger also changed**: Auto Health Export now syncs to Google
Drive instead of POSTing to a custom HTTPS endpoint. This drops the Azure
Function entirely — no API key, no Function App to host, no managed
identity chain for that hop — in exchange for depending on a Databricks
**Beta** connector (Google Drive Unity Catalog connections aren't SQL
Warehouse-compatible, notebook/cluster access only, which is part of why the
serverless-notebook path was the one built out).

**What this rebuild changed vs. the original design** (see the original
Layer 1 below for the design being traded off against):
- **Dedup is reimplemented, differently keyed.** The original Lakeflow
  pipeline deduped overlapping/re-synced exports on `(metric_name, date)` via
  Auto CDC, keyed by `file_modification_time`. `base_healthkit_metrics.sql`
  does the same dedup with a `qualify row_number() ... = 1` window function,
  keyed on `_ingested_at` (the Auto Loader's `current_timestamp()` at
  processing time) instead of file mtime — same "most recent wins" semantics,
  different recency signal, no separate CDC/merge infrastructure needed.
- **The Bronze and Gold notebooks are in version control**: both
  `databricks/free_edition_notebooks/bronze_ingest.py` and `run_dbt_gold.py`
  are exported from/imported to the live workspace via `databricks workspace
  export`/`import`, rather than deployed through Databricks Asset Bundles the
  way the Lakeflow pipeline was.
- **CI is current**: `.github/workflows/dbt.yml` authenticates with the same
  token auth `profiles.yml` uses locally, via a `DATABRICKS_TOKEN` repo
  secret — verified with a real PR against the new workspace (caught and
  fixed a real CTE syntax bug in `base_healthkit_metrics.sql` in the
  process). Runs on every PR touching `dbt/**` and on every push to `main`.
- **Gold is scheduled**, not just Bronze: a second Databricks Job
  (`healthkit-gold-daily-refresh`) clones the repo fresh and runs
  `dbt build` daily at 06:00 Europe/Copenhagen, an hour after Bronze lands.

---

## Original design (Azure + Lakeflow, retired 2026-07-18)

The rest of this document describes the first version of this pipeline in
full, kept intentionally rather than deleted: it's the more hardened
architecture (managed identity throughout, zero long-lived secrets, proper
SCD1 dedup via Auto CDC) and demonstrates a different set of skills than the
Free Edition rebuild above. Everything below was real and verified at the
time — see [`INFRA_STATUS.md`](INFRA_STATUS.md) for exactly what was torn
down and why.

Nothing here is a diagram-only architecture — every layer ran against real
production data (7,000+ real HealthKit datapoints spanning April 2025–July
2026) and every test/CI check referenced below actually passed, before the
infrastructure it depended on was deleted.

### Layer 0: Ingestion (Azure Function → Bronze)

**Code:** `healthkit-ingest-func/function_app.py`, `validation.py`

The Function receives one POST per sync from the iOS app at `/api/ingest`.

**Auth:** a custom `X-Api-Key` header, compared with `hmac.compare_digest`
against a secret pulled from Key Vault via a Key Vault reference app setting
(never stored as a plain app setting or in code).

**Validation (`validation.py`) — this is deliberately shallow, not a data
quality gate:**
- body must be non-empty, valid JSON, and a JSON object
- must have a `data` object containing a non-empty `metrics` array
- each metric object must have a `name` and a `data` array
- `workouts`, if present, must be an array
- **Nothing inside `metrics[].data[]` is validated** — dates, `qty`, `avg`,
  `min`, `max` are not type- or range-checked at this stage. The Function's
  job is "is this a well-formed Health Auto Export payload," not "is this
  good data." Malformed *individual datapoints* are caught later, in Silver.

**What lands in Bronze:** the **raw request body, byte-for-byte, unmodified**
— no re-serialization, no field extraction. Path:
`bronze/raw/healthkit/{yyyy}/{MM}/{dd}/{yyyyMMdd_HHmmss}_{uuid}.json`, where
the date is the **ingestion time**, not any date inside the payload — HealthKit
data for April 2025 sent today lands under today's folder. The historical
dates live inside the JSON itself; Silver is what extracts and indexes them.

**Idempotency mechanism:** every blob gets a `content_sha256` metadata field
(hash of the whole raw body). If the iOS app retries a failed send, the
retried request produces an identically-hashed blob — downstream consumers
*could* dedup on that hash to catch exact network retries. In practice, the
dedup that actually matters (overlapping/repeated manual exports covering the
same dates with re-serialized-but-different bytes) is handled in **Silver**,
not here — see below.

**Reliability:** the ADLS write itself has explicit retry/backoff
(`retry_total=4`, exponential backoff) independent of any client-side retry.
Structured JSON logs (request id, payload size, metric/workout counts, write
duration) go to Application Insights. Status codes: 200 success / 400 invalid
payload / 401 bad key / 500 storage failure (logged, not swallowed).

---

### Layer 1: Bronze → Silver (Databricks Lakeflow Declarative Pipeline)

**Code:** `databricks/healthkit_pipeline/src/healthkit_silver_pipeline.py`
**API:** `pyspark.pipelines` (`dp`) — the current Lakeflow API, successor to
the older `dlt` module.

This is where the actual **cleaning happens** — four stages, in order:

#### 1. `bronze_healthkit_raw` — ingest the files
Autoloader (`cloudFiles`) incrementally reads new JSON files from the Bronze
path as they land (tracks which files it's already processed — each file is
read exactly once, even across pipeline restarts). `multiLine=true` because
each file is **one JSON object**, not JSON-lines. Adds two Databricks-provided
file metadata columns: `source_file` and `file_modification_time` — the
latter turns out to be the single most important column in the whole
pipeline (see step 3). No field extraction yet; each row here is still one
whole file's nested JSON.

#### 2. `silver_healthkit_metrics_staging` — flatten
Two explodes:
1. `explode(data.metrics)` → one row per metric *per file* (a file with 24
   metrics becomes 24 rows)
2. `explode(metric.data)` → one row per **datapoint** (a metric with 5
   hourly readings in that file becomes 5 rows)

Then field extraction per datapoint:
- `metric_name`, `units` — straight copy from the metric object
- `date` — parsed from Health Auto Export's string format
  (`"yyyy-MM-dd HH:mm:ss xx"`, e.g. `"2026-07-07 08:00:00 +0000"`) into a
  real timestamp
- `value_qty`, `value_avg`, `value_min`, `value_max` — each cast to
  `double`. **Different metric types populate different subsets of these**:
  count-style metrics (`step_count`, `active_energy`) only ever populate
  `qty`; range-style metrics (`heart_rate`) populate `avg`/`min`/`max` and
  leave `qty` null. Nothing forces one shape.
- `value_raw` — the *entire original datapoint*, re-serialized to JSON
  string, kept as a catch-all/audit trail for metrics whose shape doesn't
  fit the four extracted value columns (e.g. `sleep_analysis`,
  `blood_pressure`)

**Data quality gate (the only row-dropping step in the whole pipeline):**
`@dp.expect_or_drop` silently drops any row where `metric_name` or `date`
came out null — e.g. a datapoint with an unparseable date string. This is a
Lakeflow "expectation": failing rows are dropped and counted in the
pipeline's metrics UI, not raised as a hard failure.

#### 3. `healthkit_metrics` — dedup to current state (Auto CDC, SCD Type 1)
This is the table Silver/Gold actually reads from. Built with
`dp.create_auto_cdc_flow`, keyed on **`(metric_name, date)`**, sequenced by
**`file_modification_time`**.

**What this buys you, concretely:** if the same `(metric_name, date)` pair
shows up in multiple Bronze files — because the user re-ran an overlapping
backfill export, or the app retried a send — Auto CDC keeps the row from
whichever file has the **latest** `file_modification_time` and discards the
rest. This is exactly what happened in practice: three separate export runs
were made from the app, two of which covered overlapping date ranges
(27 Apr 2025 → yesterday, run twice), and the resulting `healthkit_metrics`
table has zero duplicate `(metric_name, date)` rows despite that. The dedup
key is *file recency*, not "which value is more correct" — if HealthKit
itself revised a value between two exports, the later export wins, which is
the desired behavior (most recent sync = most current truth).

#### 4. `step_count_history` — SCD Type 2 demo (Auto CDC)
A second, parallel flow over the same staging data, filtered to
`metric_name = 'step_count'`, also via `create_auto_cdc_flow` but with
`stored_as_scd_type="2"` instead of `"1"`. Instead of overwriting on each new
value for a key, it **keeps every distinct version** with `__START_AT` /
`__END_AT` timestamp columns marking each version's validity window.
`track_history_column_list=["value_qty"]` means a new history row is only
created when the step count for a given date actually *changes* value (not
on every incidental re-sync). This table is a standalone demonstration of
the Auto CDC / SCD2 pattern — it is **not** currently consumed by any Gold
mart; Gold reads the SCD1 `healthkit_metrics` table for every metric,
step_count included.

#### What Silver does *not* do
Worth being precise about this for an interview: there's no unit
normalization, no outlier/range validation, no timezone reconciliation
beyond parsing the offset in the date string, and no deduplication of
multiple datapoints for the same metric+date *within a single file* (Health
Auto Export doesn't produce those in practice, so it's never come up — but
the current logic would just keep whichever the Auto CDC merge picks
non-deterministically among same-key rows in one batch).

---

## Layer 2: Silver → Gold (dbt)

**Code:** `dbt/healthkit/models/` — this layer is **current**, not historical;
it's shared by both architectures, though the staging model's actual input
changed with the rebuild (see below).

### Bronze → Silver: `base_healthkit_metrics.sql` (current)
This is the model that replaced the Lakeflow pipeline's ingest+flatten steps
(Layer 1, stages 1-2 above). Reads `workspace.healthkit.bronze_health_export`
directly and, per Bronze row (one row per exported file):
- `parse_json(data)` then `lateral variant_explode(payload:metrics)` — one
  row per metric per file
- a second `lateral variant_explode(...)` over each metric's `data` array —
  one row per individual reading
- pulls `qty`/`avg`/`min`/`max` out via VARIANT path access
  (`reading:qty::string`, cast to `double`), same shape-handling logic as
  the original (count-style metrics populate `qty`, range-style populate
  `avg`/`min`/`max`), plus `value_raw` (`to_json(reading)`) as the same
  catch-all/audit column the original design had.

VARIANT was the deliberate choice over a rigid `from_json` schema for the
same reason as the original: metric shapes are genuinely heterogeneous
(`sleep_analysis` has no `qty` at all). No dedup/merge step exists here —
see "Current architecture" at the top of this doc for what that trade-off
means in practice.

### Staging: `stg_healthkit_metrics.sql`
Reads `{{ ref('base_healthkit_metrics') }}` — originally read an external
Lakeflow-produced Silver table via `source()`; swapped to `ref()` when
Layer 1 changed, everything below this line is **unmodified** from the
original design. Does the remaining light cleanup:
- `lower(units)` — normalizes casing (`"BPM"` → `"bpm"`)
- **`to_date(substring(get_json_object(value_raw, '$.date'), 1, 10))` as
  `metric_date`** — deliberately *not* `cast(date as date)`. `date` is a UTC
  instant; truncating it directly reassigns readings near local midnight to
  the wrong calendar day for anyone not in UTC. Pulling the date substring
  straight from the original HealthKit string is correct regardless of
  warehouse session timezone. (A real bug, found and fixed against
  production data — see the original commit history.)
- **`coalesce(value_qty, value_avg) as value`** — the key simplification
  step. Since count-style metrics populate `qty` and range-style metrics
  populate `avg`, this collapses both shapes into one general-purpose
  `value` column so every downstream mart can write `sum(value)` or
  `avg(value)` without per-metric-type branching. `value_min`/`value_max`/
  `value_raw` are dropped here — they don't leave Silver.
- **`is_daily_rollup`** flag — Health Auto Export emits one extra
  daily-granularity rollup datapoint per metric per day (timestamped exactly
  local `00:00:00`), alongside genuine intraday readings. Flagged here so
  downstream aggregations (`fct_daily_activity_summary`) can prefer the
  rollup instead of summing it together with the intraday points — another
  real bug, found and fixed, that would otherwise silently double-count
  every cumulative metric.
- Filters out any residual null `metric_name`/`date` rows (belt-and-braces)

Materialized as a **view** (not a table) — cheap to keep fresh since it's
just a thin layer over Silver, and since it's a view it reflects new Bronze
rows immediately without a rebuild; only the Gold marts below need `dbt run`
to pick up new data.

### Gold marts (all materialized as tables)

**`fct_daily_activity_summary`** — one row per calendar day. Pivots a fixed
list of metrics into columns via conditional aggregation: `sum()` for
cumulative metrics (steps, active energy, exercise/stand minutes, flights
climbed, distance), `avg()` for point-in-time vitals (heart rate, resting
heart rate, HRV, weight). `metrics_present` = count of distinct metric types
with data that day — a rough per-day completeness signal.

**`fct_weekly_trends`** — built on top of the daily mart. Buckets into ISO
weeks (`date_trunc('week', metric_date)`, Monday start), sums/averages
across each week, then uses `LAG()` to pull the *prior* week's numbers into
the same row and compute both absolute and percent change for steps, active
energy, and resting heart rate. This is what surfaced, e.g., a week where
steps swung from −25.2% to +28.8% to +17.4% over three consecutive weeks.

**`fct_metric_freshness`** — not a "business" mart, a **pipeline health**
mart: per metric, datapoint count, first/last date seen, `days_with_data`
vs. the calendar span between first and last (`coverage_pct`), and
`days_since_last_data` (staleness). Answers "is every metric still syncing,"
independent of any dashboard — this is the table you'd alert on.

### Tests (currently 25/25 passing, verified at ~1M rows in Silver)
- `not_null` + `accepted_values` (fixed list of the 24 known HealthKit
  metric names) on `stg_healthkit_metrics.metric_name`
- `not_null` on `metric_date` / `value`
- `not_null` + `unique` on each mart's grain column
  (`metric_date`, `week_start`, `metric_name`)
- **Source freshness check** on the Bronze source (`healthkit_bronze.bronze_health_export`,
  originally on the Lakeflow Silver table): warns past 26 hours stale, errors
  past 50 — tuned around an expected roughly-daily sync cadence with slack
  for missed days

---

## Governance & auth — original design (Azure, keyless end-to-end)

This section documents the **original Azure architecture's** auth chain —
no plaintext secrets or storage account keys anywhere, every hop on managed
identity or OAuth, with the two real secrets (the ingest API key, the dbt
service principal's client secret) living only in Key Vault. It's the
stronger pattern of the two designs, kept here to show it even though it's
not what's currently live:

| Hop | Mechanism |
|---|---|
| Function App → ADLS (`healthkitdatalake`) | System-assigned managed identity, `Storage Blob Data Contributor` |
| Function App's own runtime/deployment storage | System-assigned managed identity (migrated off a connection string) |
| iOS app → Function App | `X-Api-Key` header, secret sourced from Key Vault reference |
| Databricks → ADLS (Bronze/Silver/Gold containers) | Access Connector (its own managed identity), `Storage Blob Data Contributor`, wired through Unity Catalog storage credentials + external locations (one per container) |
| dbt → Databricks SQL warehouse | Databricks service principal, OAuth M2M, client secret in Key Vault |
| GitHub Actions → Azure (both workflows) | OIDC federation (no stored GitHub secrets at all) — an Entra app registration with federated credentials scoped to `refs/heads/main` and `pull_request` |
| GitHub Actions → Key Vault (dbt job) | Same OIDC identity, `Key Vault Secrets User`, fetches the dbt secret at run time |

Unity Catalog structure: one catalog (`healthkit`) with schema `silver`
physically rooted in the `silver` ADLS container and schema `gold` rooted in
the `gold` container — the medallion separation is enforced at the storage
layer, not just by naming convention.

**Current design's auth, by contrast:** a single Databricks personal access
token, loaded from a gitignored `.env` via `env_var()` in `profiles.yml`.
Simpler, no Key Vault/managed-identity chain to stand up — the honest
trade-off for a $0 personal Free Edition workspace, not something to
present as equivalent hardening. Unity Catalog structure is one catalog
(`workspace`) with schemas `healthkit`, `healthkit_silver`, `healthkit_gold`.

---

## CI/CD

- `.github/workflows/function-app.yml` — `ruff` lint + `pytest` on every
  push/PR touching the Function code; deploys via
  `func azure functionapp publish` on merge to `main` only. Verified with a
  real push → real deploy, back when the Function App was live.
- `.github/workflows/dbt.yml` — `dbt build` + `dbt test` on every PR touching
  the dbt project, and on every push to `main`. Originally verified against
  the Azure workspace via Azure OIDC → Key Vault auth; repointed to the
  Free Edition workspace on 2026-07-22 using the same token auth
  `profiles.yml` uses locally (a `DATABRICKS_TOKEN` repo secret). Verified
  with a real PR — first run caught a genuine CTE syntax bug (missing comma
  between two CTEs in `base_healthkit_metrics.sql`, introduced by the dedup
  change), fixed, second run green, merged. That's CI doing its job, not
  a formality.
- Branch protection is **not** configured — GitHub's branch-protection API
  is blocked on private repos below the Pro tier; the exact `gh api` command
  to enable it once eligible is in the repo's setup notes.

---

## What's *not* automated (be ready for this question)

Current state, not the original design's. As of 2026-07-22, the list that
used to be here (Gold not scheduled, CI stale, no dedup, notebook not in
git) is closed — see "Current architecture" at the top of this document for
what replaced each. What's still genuinely open:

- No alerting is wired to `fct_metric_freshness` — the data exists to power
  an alert, but nothing currently reads it proactively.
- Silver has no reprocessing/backfill tooling beyond re-running the whole
  pipeline; there's no targeted "reprocess just this date range" path.
- No SCD Type 2 / change-history table in the current live design — see the
  interview-question answer below for what that trade-off means.
- Branch protection still isn't configured on the GitHub repo (same
  Pro-tier limitation as the original design).

## Likely interview questions and where the answer lives

- *"How do you handle duplicate/retried data?"* → `base_healthkit_metrics.sql`
  dedups on `(metric_name, date)` via `qualify row_number() over (... order by
  _ingested_at desc) = 1`, keeping whichever Bronze file was processed most
  recently — same "most recent wins" semantics as the original Lakeflow
  pipeline's Auto CDC (`sequence_by file_modification_time`), just keyed on
  Auto Loader ingestion order instead of file mtime, and implemented as a
  plain window function instead of a separate CDC/merge pipeline stage.
- *"Why medallion instead of writing straight to Gold?"* → Bronze is the
  immutable source of truth for replay/backfill; Silver is where schema and
  cleanup logic lives *once* instead of being reimplemented per mart; Gold is
  disposable and rebuildable from Silver at any time.
- *"How would you handle a HealthKit revision to old data?"* → **Original
  design**: SCD Type 1 (`healthkit_metrics`) naturally overwrites to the
  latest-synced value; the `step_count_history` SCD Type 2 table demonstrated
  full history-of-change tracking. The current live design's dedup step is
  also effectively SCD Type 1 (latest `_ingested_at` wins), but there's no
  SCD Type 2 equivalent yet — the honest answer is "no versioned history
  today," and a dbt snapshot over `base_healthkit_metrics` is the natural way
  to rebuild that if it's needed again.
- *"What would you build next?"* → wire `fct_metric_freshness` to an actual
  alert, add a dbt snapshot for change history, enable branch protection.
  (CI already has its own dedicated Databricks token, separate from local
  dev's, as of 2026-07-22 — see CI/CD above.)
