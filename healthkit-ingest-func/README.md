# healthkit-ingest-func

Azure Function (Python 3.13, Linux, Flex Consumption) that receives Apple
HealthKit exports from the [Health Auto Export](https://www.healthyapps.dev/)
iOS app via POST and lands the raw payload in ADLS Gen2 (`bronze` container)
for downstream processing.

- Resource group: `healthkit-portfolio-rg`
- Function App: `healthkit-ingest-func`
- Storage account: `healthkitdatalake`

## How it works

1. `POST /api/ingest` with header `X-Api-Key: <secret>` and the Health Auto
   Export v2 JSON body (`batchRequests` must be `false` so the whole export
   arrives in one request).
2. The function checks the API key, validates that `data.metrics` is a
   non-empty array of well-formed metric objects, and rejects anything else
   with `400`.
3. On success it writes the **raw, unmodified** request body to:
   ```
   bronze/raw/healthkit/{yyyy}/{MM}/{dd}/{yyyyMMdd_HHmmss}_{uuid}.json
   ```
   A SHA-256 hash of the body is stored as blob metadata (`content_sha256`)
   so Silver-layer processing can dedup retried sends without relying on the
   filename.
4. Structured JSON logs (request id, payload size, metric/workout counts,
   write duration) flow to Application Insights.

Auth to storage is via the Function App's managed identity
(`DefaultAzureCredential`) — no connection strings or account keys anywhere.

## Required app settings

| Setting | Value | Notes |
|---|---|---|
| `ADLS_ACCOUNT_URL` | `https://healthkitdatalake.dfs.core.windows.net` | DFS endpoint, not blob endpoint |
| `BRONZE_CONTAINER` | `bronze` | defaults to `bronze` if unset |
| `HEALTHKIT_API_KEY` | `@Microsoft.KeyVault(SecretUri=https://<vault-name>.vault.azure.net/secrets/healthkit-api-key/)` | Key Vault reference, resolved by the platform at runtime |
| `AzureWebJobsStorage` | identity-based connection (see note below) | the Function's own runtime/deployment storage, separate from `healthkitdatalake` |

> Flex Consumption apps also need a **deployment storage account** for the
> runtime itself (separate from the ADLS data lake you're writing business
> data to). To keep this key-free too, configure it as an identity-based
> connection (`AzureWebJobsStorage__accountName` + `AzureWebJobsStorage__credential=managedidentity`)
> instead of a connection string, and grant the same managed identity
> `Storage Blob Data Owner` on that account. If you provisioned the Function
> App through the portal/Bicep with a connection string already wired up,
> leave it as-is rather than migrating it under time pressure — it's a
> separate concern from the `healthkitdatalake` RBAC below.

## One-time setup

### 1. Enable managed identity on the Function App

```bash
az functionapp identity assign \
  --name healthkit-ingest-func \
  --resource-group healthkit-portfolio-rg

PRINCIPAL_ID=$(az functionapp identity show \
  --name healthkit-ingest-func \
  --resource-group healthkit-portfolio-rg \
  --query principalId -o tsv)
```

### 2. Grant the identity access to `healthkitdatalake`

```bash
STORAGE_ID=$(az storage account show \
  --name healthkitdatalake \
  --resource-group healthkit-portfolio-rg \
  --query id -o tsv)

az role assignment create \
  --assignee "$PRINCIPAL_ID" \
  --role "Storage Blob Data Contributor" \
  --scope "$STORAGE_ID"
```

RBAC role assignments can take a couple of minutes to propagate.

### 3. Key Vault for the API key

```bash
# Create the vault if it doesn't already exist
az keyvault create \
  --name healthkit-kv \
  --resource-group healthkit-portfolio-rg \
  --location <region>

# Generate and store the API key the iOS app will send
API_KEY=$(openssl rand -base64 32)
az keyvault secret set \
  --vault-name healthkit-kv \
  --name healthkit-api-key \
  --value "$API_KEY"
echo "Save this value for the Health Auto Export app config: $API_KEY"
```

Grant the Function App's identity read access to secrets. Use whichever
matches how the vault is configured:

```bash
# If the vault uses Azure RBAC (recommended)
KV_ID=$(az keyvault show --name healthkit-kv --resource-group healthkit-portfolio-rg --query id -o tsv)
az role assignment create \
  --assignee "$PRINCIPAL_ID" \
  --role "Key Vault Secrets User" \
  --scope "$KV_ID"

# If the vault uses classic access policies instead
az keyvault set-policy \
  --name healthkit-kv \
  --object-id "$PRINCIPAL_ID" \
  --secret-permissions get list
```

### 4. Wire the app settings

```bash
az functionapp config appsettings set \
  --name healthkit-ingest-func \
  --resource-group healthkit-portfolio-rg \
  --settings \
    ADLS_ACCOUNT_URL="https://healthkitdatalake.dfs.core.windows.net" \
    BRONZE_CONTAINER="bronze" \
    HEALTHKIT_API_KEY="@Microsoft.KeyVault(SecretUri=https://healthkit-kv.vault.azure.net/secrets/healthkit-api-key/)"
```

Confirm the Key Vault reference resolved (should show `Resolved` rather
than an error):

```bash
az functionapp config appsettings list \
  --name healthkit-ingest-func \
  --resource-group healthkit-portfolio-rg \
  --query "[?name=='HEALTHKIT_API_KEY']"
```

If it doesn't resolve, it's almost always the Key Vault RBAC/policy grant
from step 3 not having propagated yet, or a typo in the vault name/secret
name in the `SecretUri`.

### 5. Configure the `bronze` container

Make sure the container exists (create once, it doesn't need to be created
per-day — the function creates the date-partitioned path on write):

```bash
az storage container create \
  --account-name healthkitdatalake \
  --name bronze \
  --auth-mode login
```

## Deploy

```bash
cd healthkit-ingest-func
func azure functionapp publish healthkit-ingest-func
```

## Configuring the Health Auto Export app

- URL: `https://healthkit-ingest-func.azurewebsites.net/api/ingest`
- Method: `POST`
- Header: `X-Api-Key: <the value generated in step 3>`
- Export format: JSON, Export Version v2
- `batchRequests`: off (so metrics arrive in a single POST, matching the
  validation logic here)
- Frequency: hourly (or whatever cadence you configure in the app)

## Local development

```bash
cp local.settings.json.example local.settings.json
# edit local.settings.json with a real ADLS_ACCOUNT_URL and a throwaway
# HEALTHKIT_API_KEY for local testing; az login first so DefaultAzureCredential
# can fall back to your own Azure CLI credentials
func start
```

```bash
curl -X POST http://localhost:7071/api/ingest \
  -H "X-Api-Key: local-dev-only-placeholder" \
  -H "Content-Type: application/json" \
  -d @sample_payload.json
```

## Tests

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements-dev.txt
pytest
```

Tests cover the validation/parsing path only (`validation.py`) — that's
where malformed-payload bugs actually hide. They don't mock the Azure SDK.

## Notes on reliability

- **Idempotent retries**: the app may resend a batch after a network
  failure. Each write still lands as a new blob (uuid-suffixed path), but
  every blob carries a `content_sha256` metadata field, so Silver-layer
  processing can dedup on that hash rather than failing/duplicating.
- **Storage retries**: `DataLakeServiceClient` is configured with explicit
  `retry_total`/`retry_backoff_factor`/`retry_backoff_max` so transient
  Azure errors on the write itself are retried with backoff, independent of
  any retry the HTTP caller does.
- **Large payloads**: the raw body is written to storage as-is — it's
  parsed once for validation but never re-serialized, so there's no second
  in-memory transform of a potentially tens-of-MB GPS workout payload.
  `functionTimeout` in `host.json` is set to 10 minutes; raise it (Flex
  Consumption supports longer) if GPS-heavy exports start timing out.
- **Status codes**: `200` success, `400` invalid payload (with a JSON body
  describing what failed), `401` bad/missing API key, `500` on storage
  write failure (logged with the request id, not swallowed).
