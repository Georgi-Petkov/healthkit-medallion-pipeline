# HealthKit Medallion Pipeline — Architecture & Interview Reference

End-to-end pipeline that takes Apple HealthKit exports from the "Health Auto
Export" iOS app and turns them into queryable analytics tables, following the
Bronze / Silver / Gold ("medallion") pattern.

```
iPhone (Health Auto Export app)
        │  POST, JSON, X-Api-Key header
        ▼
Azure Function (healthkit-ingest-func)         ← validates, writes raw
        │
        ▼
BRONZE   adls://healthkitdatalake/bronze/raw/healthkit/{yyyy}/{MM}/{dd}/*.json
        │  Databricks Lakeflow Declarative Pipeline (Autoloader + Auto CDC)
        ▼
SILVER   healthkit.silver.healthkit_metrics   (Delta table, Unity Catalog)
        │  dbt (staging model + gold marts)
        ▼
GOLD     healthkit.gold.fct_daily_activity_summary
         healthkit.gold.fct_weekly_trends
         healthkit.gold.fct_metric_freshness
```

Nothing here is a diagram-only architecture — every layer has run against real
production data (7,000+ real HealthKit datapoints spanning April 2025–July
2026) and every test/CI check referenced below has actually passed.

---

## Layer 0: Ingestion (Azure Function → Bronze)

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

## Layer 1: Bronze → Silver (Databricks Lakeflow Declarative Pipeline)

**Code:** `databricks/healthkit_pipeline/src/healthkit_silver_pipeline.py`
**API:** `pyspark.pipelines` (`dp`) — the current Lakeflow API, successor to
the older `dlt` module.

This is where the actual **cleaning happens** — four stages, in order:

### 1. `bronze_healthkit_raw` — ingest the files
Autoloader (`cloudFiles`) incrementally reads new JSON files from the Bronze
path as they land (tracks which files it's already processed — each file is
read exactly once, even across pipeline restarts). `multiLine=true` because
each file is **one JSON object**, not JSON-lines. Adds two Databricks-provided
file metadata columns: `source_file` and `file_modification_time` — the
latter turns out to be the single most important column in the whole
pipeline (see step 3). No field extraction yet; each row here is still one
whole file's nested JSON.

### 2. `silver_healthkit_metrics_staging` — flatten
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

### 3. `healthkit_metrics` — dedup to current state (Auto CDC, SCD Type 1)
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

### 4. `step_count_history` — SCD Type 2 demo (Auto CDC)
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

### What Silver does *not* do
Worth being precise about this for an interview: there's no unit
normalization, no outlier/range validation, no timezone reconciliation
beyond parsing the offset in the date string, and no deduplication of
multiple datapoints for the same metric+date *within a single file* (Health
Auto Export doesn't produce those in practice, so it's never come up — but
the current logic would just keep whichever the Auto CDC merge picks
non-deterministically among same-key rows in one batch).

---

## Layer 2: Silver → Gold (dbt)

**Code:** `dbt/healthkit/models/`

### Staging: `stg_healthkit_metrics.sql`
Reads `healthkit.silver.healthkit_metrics` (the deduped SCD1 table) and does
the remaining light cleanup:
- `lower(units)` — normalizes casing (`"BPM"` → `"bpm"`)
- `cast(date as date) as metric_date` — truncates the timestamp to a
  calendar day; this is the grain every Gold mart aggregates on. The
  original timestamp is kept separately as `metric_timestamp`.
- **`coalesce(value_qty, value_avg) as value`** — the key simplification
  step. Since count-style metrics populate `qty` and range-style metrics
  populate `avg`, this collapses both shapes into one general-purpose
  `value` column so every downstream mart can write `sum(value)` or
  `avg(value)` without per-metric-type branching. `value_min`/`value_max`/
  `value_raw` are dropped here — they don't leave Silver.
- Filters out any residual null `metric_name`/`date` rows (belt-and-braces;
  should already be true given Silver's expectation gate)

Materialized as a **view** (not a table) — cheap to keep fresh since it's
just a thin layer over Silver.

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

### Tests (all passing, 15/15 on last run)
- `not_null` + `accepted_values` (fixed list of the 24 known HealthKit
  metric names) on `stg_healthkit_metrics.metric_name`
- `not_null` on `metric_date` / `value`
- `not_null` + `unique` on each mart's grain column
  (`metric_date`, `week_start`, `metric_name`)
- **Source freshness check** on `healthkit.silver.healthkit_metrics`,
  keyed on `ingested_at`: warns past 26 hours stale, errors past 50 — tuned
  around an expected roughly-daily sync cadence with slack for missed days

---

## Governance & auth (Unity Catalog, keyless end-to-end)

No plaintext secrets or storage account keys anywhere in this system. Each
hop uses managed identity or OAuth, with the one real secret (the ingest
API key, and the dbt service principal's client secret) living only in
Key Vault:

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

---

## CI/CD

- `.github/workflows/function-app.yml` — `ruff` lint + `pytest` on every
  push/PR touching the Function code; deploys via
  `func azure functionapp publish` on merge to `main` only. Verified with a
  real push → real deploy.
- `.github/workflows/dbt.yml` — `dbt build` + `dbt test` on every PR
  touching the dbt project. Verified with a real test PR (opened, CI ran
  green, merged).
- Branch protection is **not** configured — GitHub's branch-protection API
  is blocked on private repos below the Pro tier; the exact `gh api` command
  to enable it once eligible is in the repo's setup notes.

---

## What's *not* automated (be ready for this question)

- **The Lakeflow pipeline runs on-demand** (`databricks bundle run
  healthkit_silver_pipeline`), not on a schedule or file-arrival trigger.
  New Bronze files sit unprocessed until someone (or a Databricks Job
  trigger, not yet built) runs it.
- No alerting is wired to `fct_metric_freshness` — the data exists to power
  an alert, but nothing currently reads it proactively.
- Silver has no reprocessing/backfill tooling beyond re-running the whole
  pipeline; there's no targeted "reprocess just this date range" path.

## Likely interview questions and where the answer lives

- *"How do you handle duplicate/retried data?"* → Bronze `content_sha256`
  for exact byte-level retries; Silver's Auto CDC `sequence_by
  file_modification_time` dedup for the more realistic case of overlapping
  exports (this is the one that was actually exercised in production).
- *"Why medallion instead of writing straight to Gold?"* → Bronze is the
  immutable source of truth for replay/backfill; Silver is where schema and
  dedup logic lives *once* instead of being reimplemented per mart; Gold is
  disposable and rebuildable from Silver at any time.
- *"How would you handle a HealthKit revision to old data?"* → SCD Type 1
  (`healthkit_metrics`) naturally overwrites to the latest-synced value.
  If full history of *why* a value changed is needed, that's exactly what
  the `step_count_history` SCD Type 2 table demonstrates — extending that
  pattern to other metrics is a config change, not new code.
- *"What would you build next?"* → schedule/trigger the pipeline, wire
  `fct_metric_freshness` to an actual alert, add branch protection once the
  repo is off the free tier.
