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

# Every step below asserts its own success explicitly and aborts with a
# clear message on anything unexpected, rather than letting a failed
# lookup fall through as an empty/null value that then gets silently
# treated as "everything's fine" by a downstream comparison.
die() { echo "FATAL: $*" >&2; exit 1; }

run_sql() {
  # Runs a SQL statement and prints the raw response, aborting loudly if
  # the statement itself didn't succeed (a non-2xx HTTP response already
  # aborts via `set -e`; this catches the case where the API call
  # succeeds but the SQL statement it ran failed).
  local statement="$1" resp
  resp=$(databricks api post /api/2.0/sql/statements --profile "$PROFILE" --json "{
    \"warehouse_id\": \"$WAREHOUSE_ID\",
    \"statement\": $(jq -Rs . <<< "$statement"),
    \"wait_timeout\": \"30s\"
  }")
  local state
  state=$(jq -r '.status.state' <<< "$resp")
  [ "$state" = "SUCCEEDED" ] || die "SQL statement did not succeed (state=$state): $(jq -c '.status.error // empty' <<< "$resp")"
  echo "$resp"
}

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
[ "$state" = "RUNNING" ] || die "warehouse $WAREHOUSE_ID did not reach RUNNING (last state: $state) -- refusing to guess freshness without it"

echo "==> ensuring $STATUS_TABLE exists with current threshold logic"
run_sql "CREATE OR REPLACE VIEW $STATUS_TABLE AS SELECT CASE WHEN MAX(days_since_last_data) >= 3 THEN 'BLOCKED' ELSE 'OK' END AS status, MAX(days_since_last_data) AS max_days_since_last_data, CURRENT_TIMESTAMP() AS checked_at FROM workspace.healthkit_gold.fct_metric_freshness WHERE metric_name IN ('step_count', 'active_energy', 'sleep_analysis')" > /dev/null

echo "==> checking $STATUS_TABLE"
status_json=$(run_sql "SELECT status, max_days_since_last_data FROM $STATUS_TABLE")
row_count=$(jq -r '.result.row_count // (.result.data_array | length)' <<< "$status_json")
[ "$row_count" = "1" ] || die "expected exactly 1 row from $STATUS_TABLE, got '$row_count' -- refusing to guess"
status=$(jq -r '.result.data_array[0][0]' <<< "$status_json")
max_days=$(jq -r '.result.data_array[0][1]' <<< "$status_json")
case "$status" in
  OK|BLOCKED) ;;
  *) die "unexpected status value '$status' from $STATUS_TABLE -- expected OK or BLOCKED" ;;
esac
echo "    status=$status max_days_since_last_data=$max_days"

desired_tables=$FULL_TABLES
exit_code=0
if [ "$status" = "BLOCKED" ]; then
  desired_tables=$BLOCKED_TABLES
  exit_code=2
fi

echo "==> fetching current space config"
space_json=$(databricks api get "/api/2.0/genie/spaces/$SPACE_ID?include_serialized_space=true" --profile "$PROFILE")
current_serialized=$(jq -r '.serialized_space' <<< "$space_json")
[ -n "$current_serialized" ] && [ "$current_serialized" != "null" ] || die "get-space returned no serialized_space -- check include_serialized_space=true and CAN EDIT permission"
current_tables=$(jq -ce '.data_sources.tables | sort_by(.identifier)' <<< "$current_serialized") || die "serialized_space.data_sources.tables was missing or malformed"
desired_tables_sorted=$(jq -ce 'sort_by(.identifier)' <<< "$desired_tables")

if [ "$current_tables" = "$desired_tables_sorted" ]; then
  echo "==> already in the correct state ($status), no update needed"
  exit "$exit_code"
fi

echo "==> updating space: $( [ "$status" = "BLOCKED" ] && echo "BLOCKING down to $STATUS_TABLE only" || echo "RESTORING full table access" )"
new_serialized=$(jq --argjson tables "$desired_tables_sorted" '.data_sources.tables = $tables' <<< "$current_serialized")
# NOTE: etag conflict-detection on this CLI (v1.6.0, Genie is Beta) rejects
# etags that were just independently re-verified as current -- looks like an
# eventual-consistency quirk between the read and write paths, not a real
# concurrent edit. This is a single-user space, so we skip conflict
# detection entirely per the CLI's own suggested workaround rather than
# chase a platform bug: "omit the etag to skip conflict detection." This is
# exactly why the verification step below re-fetches and checks the result
# instead of trusting this call's exit code alone.
update_body=$(jq -n --arg serialized "$new_serialized" '{serialized_space: $serialized}')
databricks genie update-space "$SPACE_ID" --profile "$PROFILE" --json "$update_body" -o json | jq '{etag, title}'

echo "==> verifying the update actually took effect"
verify_tables=$(databricks api get "/api/2.0/genie/spaces/$SPACE_ID?include_serialized_space=true" --profile "$PROFILE" \
  | jq -ce '.serialized_space | fromjson | .data_sources.tables | sort_by(.identifier)')
[ "$verify_tables" = "$desired_tables_sorted" ] || die "update-space reported success but re-fetching shows the wrong tables -- got: $verify_tables"
echo "    confirmed: space now has exactly the expected tables"

exit "$exit_code"
