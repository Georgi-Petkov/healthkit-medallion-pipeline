#!/usr/bin/env bash
# Hard freshness gate for the HealthKit Genie space.
#
# Genie's "run as" identity on this workspace is the account owner, who
# bypasses Unity Catalog ACLs -- so a REVOKE SELECT on the gold/silver
# schemas is a no-op here (confirmed: `databricks grants get schema
# workspace.healthkit_gold` returns no explicit grants at all). Blocking
# has to happen at the Genie space's own data_sources.tables list instead,
# since that's not an ACL and ownership can't bypass it: when stale, this
# script points the space at ONLY workspace.healthkit_gold.genie_status
# (a one-row live view derived from fct_metric_freshness) so Genie
# structurally has nothing else to query; when fresh again, it restores
# the full table list.
#
# genie_status.status is BLOCKED when days_since_last_data >= 3 for any of
# step_count / active_energy / sleep_analysis. Health Auto Export exports a
# completed day, so days_since_last_data == 1 is the best-case steady state
# (it can never be 0 by design) and == 2 is ambiguous -- it's the expected
# reading any time before today's ~06:00 CET refresh has run yet, not
# necessarily a missed sync. Only >= 3 is unambiguous: a full extra day has
# passed since 2 would've been excusable, so a scheduled run has definitely
# failed to land fresh data.
#
# Usage: ./genie_freshness_gate.sh
# Exit code: 0 = fresh (space left/restored to full access)
#            2 = stale (space blocked down to genie_status only)
set -euo pipefail

PROFILE="healthkit"
WAREHOUSE_ID="9331fd1235b63aa4"
SPACE_ID="01f1a1f4b2c01040937125143731525f"
STATUS_TABLE="workspace.healthkit_gold.genie_status"

FULL_TABLES='[
  {"identifier": "workspace.healthkit.bronze_health_export"},
  {"identifier": "workspace.healthkit_gold.fct_daily_activity_summary"},
  {"identifier": "workspace.healthkit_gold.fct_metric_freshness"},
  {"identifier": "workspace.healthkit_gold.fct_weekly_trends"},
  {"identifier": "workspace.healthkit_gold.fct_weekly_weight_trends"},
  {"identifier": "workspace.healthkit_silver.base_healthkit_metrics"},
  {"identifier": "workspace.healthkit_silver.stg_healthkit_metrics"}
]'
BLOCKED_TABLES="[{\"identifier\": \"$STATUS_TABLE\"}]"

echo "==> ensuring warehouse is running"
state=$(databricks warehouses get "$WAREHOUSE_ID" --profile "$PROFILE" -o json | jq -r '.state')
if [ "$state" != "RUNNING" ]; then
  databricks warehouses start "$WAREHOUSE_ID" --profile "$PROFILE" -o json > /dev/null
  for _ in $(seq 1 12); do
    sleep 5
    state=$(databricks warehouses get "$WAREHOUSE_ID" --profile "$PROFILE" -o json | jq -r '.state')
    [ "$state" = "RUNNING" ] && break
  done
fi

echo "==> ensuring $STATUS_TABLE exists with current threshold logic"
databricks api post /api/2.0/sql/statements --profile "$PROFILE" --json "{
  \"warehouse_id\": \"$WAREHOUSE_ID\",
  \"statement\": \"CREATE OR REPLACE VIEW $STATUS_TABLE AS SELECT CASE WHEN MAX(days_since_last_data) >= 3 THEN 'BLOCKED' ELSE 'OK' END AS status, MAX(days_since_last_data) AS max_days_since_last_data, CURRENT_TIMESTAMP() AS checked_at FROM workspace.healthkit_gold.fct_metric_freshness WHERE metric_name IN ('step_count', 'active_energy', 'sleep_analysis')\",
  \"wait_timeout\": \"30s\"
}" | jq -e '.status.state == "SUCCEEDED"' > /dev/null

echo "==> checking $STATUS_TABLE"
status_json=$(databricks api post /api/2.0/sql/statements --profile "$PROFILE" --json "{
  \"warehouse_id\": \"$WAREHOUSE_ID\",
  \"statement\": \"SELECT status, max_days_since_last_data FROM $STATUS_TABLE\",
  \"wait_timeout\": \"30s\"
}")
status=$(echo "$status_json" | jq -r '.result.data_array[0][0]')
max_days=$(echo "$status_json" | jq -r '.result.data_array[0][1]')
echo "    status=$status max_days_since_last_data=$max_days"

desired_tables=$FULL_TABLES
exit_code=0
if [ "$status" = "BLOCKED" ]; then
  desired_tables=$BLOCKED_TABLES
  exit_code=2
fi

echo "==> fetching current space config"
space_json=$(databricks api get "/api/2.0/genie/spaces/$SPACE_ID?include_serialized_space=true" --profile "$PROFILE")
etag=$(echo "$space_json" | jq -r '.etag')
current_serialized=$(echo "$space_json" | jq -r '.serialized_space')
current_tables=$(echo "$current_serialized" | jq -c '.data_sources.tables | sort_by(.identifier)')
desired_tables_sorted=$(echo "$desired_tables" | jq -c 'sort_by(.identifier)')

if [ "$current_tables" = "$desired_tables_sorted" ]; then
  echo "==> already in the correct state ($status), no update needed"
  exit "$exit_code"
fi

echo "==> updating space: $( [ "$status" = "BLOCKED" ] && echo "BLOCKING down to $STATUS_TABLE only" || echo "RESTORING full table access" )"
new_serialized=$(echo "$current_serialized" | jq --argjson tables "$desired_tables_sorted" '.data_sources.tables = $tables')
# NOTE: etag conflict-detection on this CLI (v1.6.0, Genie is Beta) rejects
# etags that were just independently re-verified as current -- looks like an
# eventual-consistency quirk between the read and write paths, not a real
# concurrent edit. This is a single-user space, so we skip conflict
# detection entirely per the CLI's own suggested workaround rather than
# chase a platform bug: "omit the etag to skip conflict detection."
update_body=$(jq -n --arg serialized "$new_serialized" '{serialized_space: $serialized}')
databricks genie update-space "$SPACE_ID" --profile "$PROFILE" --json "$update_body" -o json | jq '{etag, title}'

exit "$exit_code"
