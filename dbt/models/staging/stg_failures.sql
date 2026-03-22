-- models/staging/stg_failures.sql
{{ config(materialized='view') }}

select
    failure_id,
    machine_id,
    timestamp_occurrence                         as occurred_at,
    DATE(timestamp_occurrence) as occurrence_date,
    failure_category,
    root_cause,
    severity_level,
    steel_grade_active,
    hardness_index_active,

    -- Machine state at time of failure
    load_index_at_failure,
    bearing_temp_c_at_failure,
    vibration_rms_at_failure,
    motor_torque_nm_at_failure,
    bearing_health_at_failure,
    lube_health_at_failure,
    align_health_at_failure,
    motor_health_at_failure,

    -- Operational context at time of failure
    demand_index,
    rush_factor,
    maintenance_deferral,
    cast(is_weekend as bool)                     as is_weekend,
    month_pulse_index,
    trend_index,

    loaded_at

from {{ source('industrial_twin_raw', 'failures') }}
where failure_id           is not null
  and machine_id           is not null
  and timestamp_occurrence is not null