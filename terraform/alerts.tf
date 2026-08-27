# Proactive freshness alert -- catches the failure class that on_failure
# email notifications (jobs.tf) can't: a job that reports SUCCESS while
# silently producing no/stale data (e.g. the Google Drive folder mixup,
# where the ingest job logged "0 new files" as a normal success every day
# for two weeks). Runs independently of both daily jobs on its own
# schedule, using its own run-as identity -- not a stored secret -- so it
# can't fail the same way healthkit-gold-daily-refresh just did.
#
# Reuses workspace.healthkit_gold.genie_status (created by
# databricks/healthkit_pipeline/scripts/genie_freshness_gate.sh), so the
# "what counts as stale" threshold lives in exactly one place and the
# Genie hard-block gate and this alert can never disagree with each other.
resource "databricks_alert_v2" "gold_data_staleness" {
  display_name = "HealthKit gold data staleness"
  query_text   = "SELECT status FROM workspace.healthkit_gold.genie_status"
  warehouse_id = "9331fd1235b63aa4"

  evaluation = {
    comparison_operator = "EQUAL"
    # A query that returns zero rows (e.g. the view got dropped, or
    # fct_metric_freshness is empty) is itself alert-worthy -- fail loud,
    # don't stay silent on an unexpected shape.
    empty_result_state = "TRIGGERED"

    source = {
      name = "status"
    }

    threshold = {
      value = {
        string_value = "BLOCKED"
      }
    }

    notification = {
      notify_on_ok      = true # also notify when it recovers, to close the loop
      retrigger_seconds = 0    # one alert per incident, not a daily repeat while still broken
      subscriptions = [
        { user_email = "2georgipetkov@gmail.com" }
      ]
    }
  }

  schedule = {
    # 1 hour after the 06:00 gold refresh, giving it time to finish.
    quartz_cron_schedule = "0 0 7 * * ?"
    timezone_id          = "Europe/Copenhagen"
    pause_status         = "UNPAUSED"
  }
}
