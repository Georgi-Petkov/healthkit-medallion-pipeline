-- Run once AFTER the backfill has been ingested and Silver/Gold look right.
-- Keeps only the latest landing of each export file in Bronze, then reclaims storage.

DELETE FROM workspace.healthkit.bronze_health_export AS b
WHERE EXISTS (
  SELECT 1
  FROM workspace.healthkit.bronze_health_export AS n
  WHERE n._source_file = b._source_file
    AND n._ingested_at > b._ingested_at
);

VACUUM workspace.healthkit.bronze_health_export;
