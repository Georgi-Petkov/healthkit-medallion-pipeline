-- Completeness test, not a validity test: catches a Bronze file that landed
-- successfully but produced zero rows downstream -- the failure mode none of
-- the row-level tests (not_null, accepted_values, etc.) can see, since they
-- only ever check rows that already exist. Found 2026-07-24: three files
-- landed in Bronze with a valid, current _ingested_at, but the JSON path in
-- base_healthkit_metrics.sql resolved to nothing for them (an extra nesting
-- level), producing zero Silver rows with no error anywhere.
with bronze_files as (

    select distinct _source_file
    from {{ source('healthkit_bronze', 'bronze_health_export') }}

),

silver_files as (

    select distinct source_file
    from {{ ref('base_healthkit_metrics') }}

)

select bronze_files._source_file
from bronze_files
left join silver_files on bronze_files._source_file = silver_files.source_file
where silver_files.source_file is null
