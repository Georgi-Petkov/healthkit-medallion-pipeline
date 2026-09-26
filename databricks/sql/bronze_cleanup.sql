-- Run once AFTER re-ingested files have landed and Silver/Gold look right.
--
-- Removes only EXACT duplicate landings: an older Bronze row for a file whose
-- payload is byte-identical to a newer landing of the same file. Older
-- landings with different content are kept on purpose -- Health Auto Export
-- configs change over time, and an older export can carry metrics a newer
-- one dropped (verified 2026-09-26: July exports include flights_climbed,
-- time_in_daylight, walking_speed; the current config does not). Silver's
-- per-(metric, timestamp) dedup already resolves overlaps between landings.

DELETE FROM workspace.healthkit.bronze_health_export AS b
WHERE EXISTS (
  SELECT 1
  FROM workspace.healthkit.bronze_health_export AS n
  WHERE n._source_file = b._source_file
    AND n._ingested_at > b._ingested_at
    AND n.data = b.data
);

VACUUM workspace.healthkit.bronze_health_export;
