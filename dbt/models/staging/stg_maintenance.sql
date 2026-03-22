-- models/staging/stg_maintenance.sql
{{ config(materialized='view') }}

select
    maintenance_id,
    machine_id,
    timestamp_start                     as started_at,
    DATE(timestamp_start)       as maintenance_date,
    maint_type,
    is_planned,
    linked_failure_id,
    duration_minutes,
    loaded_at

from {{ source('industrial_twin_raw', 'maintenance') }}
where maintenance_id  is not null
  and machine_id      is not null
  and timestamp_start is not null