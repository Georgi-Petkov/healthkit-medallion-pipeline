# These two resources bring the pipeline's two live daily Databricks Jobs
# under IaC. Both already existed (created manually via the workspace UI) —
# see terraform/README.md for the import steps used to attach them here
# without recreating or restarting either schedule.

resource "databricks_job" "bronze_ingest" {
  name = "healthkit-bronze-daily-ingest"

  max_concurrent_runs = 1

  queue {
    enabled = true
  }

  schedule {
    quartz_cron_expression = "0 0 5 * * ?"
    timezone_id            = "Europe/Copenhagen"
    pause_status           = "UNPAUSED"
  }

  task {
    task_key = "bronze_ingest"

    notebook_task {
      notebook_path = "/Users/2georgipetkov@gmail.com/bronze_ingest_v2"
      source        = "WORKSPACE"
    }
  }
}

resource "databricks_job" "gold_refresh" {
  name = "healthkit-gold-daily-refresh"

  max_concurrent_runs = 1

  queue {
    enabled = true
  }

  schedule {
    quartz_cron_expression = "0 0 6 * * ?"
    timezone_id            = "Europe/Copenhagen"
    pause_status           = "UNPAUSED"
  }

  task {
    task_key = "run_dbt_gold"

    notebook_task {
      notebook_path = "/Users/2georgipetkov@gmail.com/run_dbt_gold"
      source        = "WORKSPACE"
    }
  }
}
