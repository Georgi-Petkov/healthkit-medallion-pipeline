#!/usr/bin/env bash
#
# Best-effort reconstruction of the Azure-side infrastructure for the
# HealthKit medallion pipeline, torn down 2026-07-18 to stop a ~$37/month
# fixed NAT Gateway cost (see ../INFRA_STATUS.md and ../STATUS.md).
#
# NOT tested end-to-end - running this re-provisions real, billable
# infrastructure. Review every section before running. Expect to debug
# a step or two, most likely around Unity Catalog metastore attachment
# and Flex Consumption region availability, both fiddly the first time
# around too.
#
# Deliberately different from the original setup in ONE place: the
# Databricks workspace below is created WITHOUT Secure Cluster
# Connectivity (--enable-no-public-ip false), because SCC's NAT Gateway
# is exactly what caused the teardown this script exists to reverse.
# Flip ENABLE_NO_PUBLIC_IP below if you'd rather have that hardening
# back and accept the ~$37/month fixed cost.
#
# After this script: run `databricks bundle deploy` from
# databricks/healthkit_pipeline/ to recreate the Lakeflow pipeline and
# scheduled job (that part IS genuinely one-command and proven).

set -euo pipefail

# --- Configuration - review before running ---
SUBSCRIPTION_ID="2fade1b9-4945-4e17-b601-c64f0a708c6c"
TENANT_ID="f8d77575-6c47-4daf-864d-cb90fe3a9f45"
RESOURCE_GROUP="healthkit-portfolio-rg"
LOCATION_PRIMARY="westeurope"       # data lake + Databricks workspace
LOCATION_FUNC="swedencentral"       # Function App + its storage + Key Vault (matches original setup)

STORAGE_ACCOUNT_DATALAKE="healthkitdatalake"              # may need a new globally-unique name if this one is taken post-deletion
STORAGE_ACCOUNT_FUNC="healthkitportfoliorae54"             # ditto
KEY_VAULT_NAME="healthkit-ingest-kv"                        # ditto - Key Vault names are globally unique and soft-deleted vaults can block reuse for up to 90 days, see note below
FUNCTION_APP_NAME="healthkit-ingest-func"
DATABRICKS_WORKSPACE_NAME="healthkit-databricks"
DATABRICKS_CONNECTOR_NAME="healthkit-databricks-connector"

ENABLE_NO_PUBLIC_IP="false"   # deliberately different from the original (was implicitly true/default) - see header comment

echo "=== Selecting subscription ==="
az account set --subscription "$SUBSCRIPTION_ID"

echo "=== Resource group ==="
az group create --name "$RESOURCE_GROUP" --location "$LOCATION_PRIMARY"

# --- Data lake storage account (ADLS Gen2, hierarchical namespace) ---
echo "=== Storage account: $STORAGE_ACCOUNT_DATALAKE ==="
az storage account create \
  --name "$STORAGE_ACCOUNT_DATALAKE" \
  --resource-group "$RESOURCE_GROUP" \
  --location "$LOCATION_PRIMARY" \
  --sku Standard_LRS \
  --kind StorageV2 \
  --hierarchical-namespace true

for CONTAINER in bronze silver gold; do
  az storage container create \
    --account-name "$STORAGE_ACCOUNT_DATALAKE" \
    --name "$CONTAINER" \
    --auth-mode login
done

# --- Key Vault (RBAC authorization, not access policies) ---
echo "=== Key Vault: $KEY_VAULT_NAME ==="
# NOTE: if this name was soft-deleted less than 90 days ago, creation will
# fail - either `az keyvault recover --name "$KEY_VAULT_NAME"` or pick a
# new name.
az keyvault create \
  --name "$KEY_VAULT_NAME" \
  --resource-group "$RESOURCE_GROUP" \
  --location "$LOCATION_FUNC" \
  --enable-rbac-authorization true

echo "TODO: create secret 'healthkit-api-key' in $KEY_VAULT_NAME manually (the value itself was never stored in this repo, by design)."

# --- Function App's own runtime storage (identity-based auth, not connection strings) ---
echo "=== Function App runtime storage: $STORAGE_ACCOUNT_FUNC ==="
az storage account create \
  --name "$STORAGE_ACCOUNT_FUNC" \
  --resource-group "$RESOURCE_GROUP" \
  --location "$LOCATION_FUNC" \
  --sku Standard_LRS \
  --kind StorageV2

# --- Function App, Flex Consumption, Python 3.13 ---
echo "=== Function App: $FUNCTION_APP_NAME (Flex Consumption) ==="
# Confirm Flex Consumption is available in $LOCATION_FUNC before running this -
# it wasn't available in westeurope at original build time, hence swedencentral.
az functionapp create \
  --name "$FUNCTION_APP_NAME" \
  --resource-group "$RESOURCE_GROUP" \
  --storage-account "$STORAGE_ACCOUNT_FUNC" \
  --flexconsumption-location "$LOCATION_FUNC" \
  --runtime python \
  --runtime-version 3.13

echo "=== Migrating Function App to identity-based storage auth ==="
az functionapp identity assign --name "$FUNCTION_APP_NAME" --resource-group "$RESOURCE_GROUP"
FUNC_PRINCIPAL_ID=$(az functionapp identity show --name "$FUNCTION_APP_NAME" --resource-group "$RESOURCE_GROUP" --query principalId -o tsv)

az functionapp config appsettings set \
  --name "$FUNCTION_APP_NAME" \
  --resource-group "$RESOURCE_GROUP" \
  --settings "AzureWebJobsStorage__accountName=$STORAGE_ACCOUNT_FUNC"

az functionapp deployment config set \
  --name "$FUNCTION_APP_NAME" \
  --resource-group "$RESOURCE_GROUP" \
  --deployment-storage-auth-type SystemAssignedIdentity

echo "=== RBAC: Function App managed identity ==="
DATALAKE_ID=$(az storage account show --name "$STORAGE_ACCOUNT_DATALAKE" --resource-group "$RESOURCE_GROUP" --query id -o tsv)
FUNCSTORAGE_ID=$(az storage account show --name "$STORAGE_ACCOUNT_FUNC" --resource-group "$RESOURCE_GROUP" --query id -o tsv)
KV_ID=$(az keyvault show --name "$KEY_VAULT_NAME" --resource-group "$RESOURCE_GROUP" --query id -o tsv)

az role assignment create --assignee "$FUNC_PRINCIPAL_ID" --role "Storage Blob Data Contributor" --scope "$DATALAKE_ID"
az role assignment create --assignee "$FUNC_PRINCIPAL_ID" --role "Storage Blob Data Owner" --scope "$FUNCSTORAGE_ID"
az role assignment create --assignee "$FUNC_PRINCIPAL_ID" --role "Key Vault Secrets User" --scope "$KV_ID"

# --- Databricks workspace (Premium tier, Unity Catalog auto-enabled) ---
echo "=== Databricks workspace: $DATABRICKS_WORKSPACE_NAME ==="
az databricks workspace create \
  --name "$DATABRICKS_WORKSPACE_NAME" \
  --resource-group "$RESOURCE_GROUP" \
  --location "$LOCATION_PRIMARY" \
  --sku premium \
  --enable-no-public-ip "$ENABLE_NO_PUBLIC_IP"

echo "TODO (manual, one-time, via Databricks account console):"
echo "  1. Attach this workspace to the existing regional metastore (likely 'metastore_azure_westeurope' if it still exists"
echo "     from before - check the account console's Catalog page before creating a new one)."
echo "  2. If the 'healthkit' catalog/schemas still exist in that metastore from before deletion, they should just"
echo "     become visible again once attached - verify with a SELECT before assuming you need to recreate them."
echo "  3. If they don't exist, recreate: catalog 'healthkit', schemas 'silver' and 'gold'."

# --- Unity Catalog Access Connector (managed identity for ADLS access) ---
echo "=== Access Connector: $DATABRICKS_CONNECTOR_NAME ==="
az databricks access-connector create \
  --name "$DATABRICKS_CONNECTOR_NAME" \
  --resource-group "$RESOURCE_GROUP" \
  --location "$LOCATION_PRIMARY" \
  --identity-type SystemAssigned

CONNECTOR_PRINCIPAL_ID=$(az databricks access-connector show --name "$DATABRICKS_CONNECTOR_NAME" --resource-group "$RESOURCE_GROUP" --query identity.principalId -o tsv)
az role assignment create --assignee "$CONNECTOR_PRINCIPAL_ID" --role "Storage Blob Data Contributor" --scope "$DATALAKE_ID"

echo "TODO (manual, via Databricks SQL once workspace + metastore attachment are live):"
echo "  1. Create storage credential 'healthkit-storage-credential' using this access connector's resource ID."
echo "  2. Create external locations 'healthkit_bronze_loc', 'healthkit_silver_loc', 'healthkit_gold_loc' pointing at"
echo "     abfss://{bronze,silver,gold}@${STORAGE_ACCOUNT_DATALAKE}.dfs.core.windows.net/ using that storage credential."
echo "  3. Create SQL warehouse 'healthkit-dbt-warehouse': serverless, 2X-Small, auto_stop_mins=10."
echo "  4. Create/reuse Databricks service principal 'dbt-healthkit-ci' for dbt OAuth M2M auth; generate a new OAuth"
echo "     secret (the old one, app id 6b45a84c-46ce-4399-9b6a-44bcae35bf65, may or may not still be valid depending"
echo "     on whether the service principal itself survived - verify in the account console); store the secret as"
echo "     '${KEY_VAULT_NAME}/dbt-databricks-client-secret'."
echo "  5. Grant that service principal USE CATALOG / USE SCHEMA / SELECT / MODIFY on the healthkit catalog."
echo "  6. Update dbt/healthkit/profiles.yml and databricks/healthkit_pipeline/resources/*.yml with the new"
echo "     workspace host / warehouse_id / pipeline_id / job git_source if any of these changed from the originals"
echo "     documented in project memory (adb-7405605320524740.0.azuredatabricks.net, warehouse 997b45263de388bd)."

echo
echo "=== Done with Azure-side provisioning ==="
echo "Next: cd databricks/healthkit_pipeline && databricks bundle deploy"
echo "Then: redeploy Function App code via the function-app.yml GitHub Actions workflow"
