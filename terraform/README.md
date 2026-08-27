# Terraform

Manages the two live daily Databricks Jobs (`healthkit-bronze-daily-ingest`,
`healthkit-gold-daily-refresh`) plus a freshness alert, as code, on the same
Databricks Free Edition workspace the rest of this repo runs on.

## Scope

`databricks_job` and `databricks_alert_v2` resources against the existing
workspace. The two jobs each have `email_notifications.on_failure` set, so
a job that errors (like `healthkit-gold-daily-refresh` did for 3 days in
Aug 2026 on a stale credential) actually notifies someone instead of
failing in silence. The alert (`gold_data_staleness`) covers the other
failure class those can't: a job that reports SUCCESS while silently
producing no/stale data — see the comment in `alerts.tf` for how it works.
No cloud provider resources (AWS/Azure/GCP), no account-level Databricks
resources — Free Edition doesn't expose an account console or account API,
so none of that is reachable from here anyway. Nothing here can incur
cost: Free Edition has no billing attached (quota-throttled, not
metered), and this config only touches job/alert *definitions*, it
doesn't create new compute.

## Auth

Uses the existing `healthkit` profile in `~/.databrickscfg` (the same one
the `databricks` CLI already uses) — no token is stored in this repo.

## State

Local state only (`terraform.tfstate`, gitignored) — this is a single-operator
project, so there's no need for a remote backend or state locking.

## History

Both jobs already existed (created manually via the workspace UI) before
Terraform was introduced. They were brought under management with
`terraform import`, not recreated — recreating them would have produced
duplicate jobs and doubled the daily ingestion/dbt runs. If you ever need to
redo this (e.g. a fresh workspace), find the job IDs with
`databricks jobs list --profile healthkit` and import each:

```
terraform import databricks_job.bronze_ingest <job_id>
terraform import databricks_job.gold_refresh <job_id>
```

Then run `terraform plan` and confirm it reports no changes before trusting
the config — that's the signal the HCL actually matches what's live.
