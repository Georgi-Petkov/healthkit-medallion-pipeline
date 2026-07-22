# Infrastructure status: rebuilt on Databricks Free Edition, $0/month (2026-07-22)

**Current state: live and running, for free.** After the Azure teardown documented below, the pipeline was rebuilt from scratch on Databricks Free Edition — new ingestion path (Google Drive connector, replacing the Azure Function), new Bronze→Silver transform (dbt with Databricks VARIANT explode, replacing the Lakeflow Declarative Pipeline that depended on classic clusters Free Edition doesn't offer). Verified end-to-end at real scale: ~1M rows through Silver, all 25 dbt tests passing, Bronze ingestion scheduled daily. Full technical detail in [`PIPELINE_ARCHITECTURE.md`](PIPELINE_ARCHITECTURE.md)'s "Current architecture" section — including the capability trade-offs (no dedup step, CI not yet repointed, notebook not yet in git) that came with the free rebuild.

The rest of this document is kept as-is: an accurate historical record of why and how the original Azure infrastructure was torn down on 2026-07-18. Nothing below describes the current running system.

---

## Original teardown (2026-07-18)

All Azure infrastructure for this project was **intentionally deleted** on 2026-07-18 to stop an unexpected recurring cost. This was a deliberate decision, not an accident or outage — if you're reading this wondering why nothing works, this is why, and how to bring it back (or how it *was* brought back — see the current-state note above).

## Why

The Azure Databricks workspace (`healthkit-databricks`) used Secure Cluster Connectivity (No Public IP) with Databricks' auto-managed VNet, which requires a NAT Gateway for cluster egress. That NAT Gateway billed a flat ~$37/month **regardless of usage** — it cost the same whether the pipeline ran constantly or was never touched. This was discovered via an Azure budget alert and confirmed via day-by-day Cost Management data (a perfectly flat daily rate, the signature of a fixed resource charge, not usage-based billing). It couldn't be disabled in place (Microsoft documents that toggle as a temporary-rollback-only mechanism, being phased out) or removed directly (it lives in a Databricks-managed resource group protected by a system deny assignment). The only reliable way to stop it was deleting the workspace — and given that was already necessary, everything else in the project's resource group was torn down too rather than leaving smaller residual costs (storage, Key Vault, Function App) running for a paused project.

## What was deleted

- Resource group `healthkit-portfolio-rg` (West Europe / Sweden Central), including:
  - `healthkit-databricks` (Databricks workspace, Unity Catalog access)
  - `healthkitdatalake` (storage account — **all Bronze/Silver/Gold HealthKit data**, not preserved)
  - `healthkit-ingest-kv` (Key Vault)
  - `healthkit-ingest-func` (ingestion Function App) + its Application Insights + App Service Plan
  - `healthkit-databricks-connector` (Unity Catalog access connector)
  - `healthkitportfoliorae54` (supporting storage account)
- The Databricks-managed resource group `databricks-rg-healthkit-databricks-9jddutcvff759` (NAT Gateway, public IP, DBFS storage, VNet, NSG) — required a separate deletion once the workspace itself was gone (a system deny assignment blocks direct deletion while the workspace exists).

Confirmed via subscription-wide resource scan: zero resources with "healthkit" in the name or resource group remain anywhere in the subscription as of this writing.

**Not explicitly cleaned up**: the Unity Catalog metastore/catalog registration is a Databricks control-plane object, separate from Azure billing, not deleted by the above (and not reachable without a live workspace to check via SQL). It carries no Azure cost on its own, so this wasn't pursued further — but be aware `healthkit` catalog metadata may still be registered at the account level if you ever recreate a workspace and attach it to the same metastore.

## What survives

**Nothing.** This was a full teardown by explicit instruction — no data preservation, no partial keep-list. All HealthKit data in the data lake is gone. The only thing that survives is the code and configuration in this repository (and its `MyHealthData` backup copy), which is why redeployment below is "recreate from scratch," not "restore."

## How to redeploy from scratch, if resumed later

**Note: this plan was not the path actually taken.** The project resumed on
Databricks Free Edition instead (see the top of this document) — cheaper but
not a like-for-like restore of this Azure architecture. The steps below are
kept for reference if a real Azure redeploy is ever wanted again (e.g. to
restore the stronger managed-identity/Key Vault auth chain, or the Auto CDC
dedup that the Free Edition rebuild doesn't have).

1. Recreate the resource group and its resources (storage account with `bronze`/`silver`/`gold` containers, Key Vault, Function App, Databricks workspace) — none of this is currently defined as reusable IaC beyond the Databricks Asset Bundle (`databricks/healthkit_pipeline/`), so the Azure-native resources (storage, Key Vault, Function App, the workspace itself) need to be provisioned manually or scripted fresh.
2. **Decide deliberately on the networking configuration this time** — the default (Secure Cluster Connectivity with a Databricks-managed VNet) is what caused this teardown. Consider explicitly setting `enableNoPublicIp: false` at creation time if cost matters more than the hardening it provides for a personal portfolio project, understanding Microsoft is moving toward making SCC mandatory long-term.
3. Attach the new workspace to a Unity Catalog metastore (new or pre-existing, see the note above about possibly-orphaned `healthkit` catalog metadata) and recreate the `healthkit` catalog with `silver`/`gold` schemas and External Locations pointing at the new storage account.
4. Set up the Access Connector + storage role assignment for Unity Catalog to reach the storage account.
5. Re-run `databricks bundle deploy` from `databricks/healthkit_pipeline/` — the pipeline (`resources/healthkit_pipeline.yml`) and job (`resources/healthkit_daily_job.yml`) definitions are unchanged and ready to redeploy as-is.
6. Recreate the Key Vault secret for the dbt service principal's OAuth client secret; re-run the CI OIDC federation setup for GitHub Actions if the app registration was also removed (it wasn't part of this teardown — check `az ad app list` — but its role assignments on the now-deleted resources will need to be redone against the new ones).
7. Re-deploy the ingestion Function App (`healthkit-ingest-func/`) via the existing `function-app.yml` CI workflow.
8. All data starts empty — there is no historical HealthKit data to backfill from, since it was deleted along with the storage account. New data accumulates from whenever ingestion resumes.
