-- models/staging/stg_telemetry.sql
{{ config(materialized='view') }}

select
    timestamp                                   as recorded_at,
    DATE(timestamp)                             as recording_date, 
    machine_id,
    shift_id,
    status,
    cast(is_running  as BOOL)                   as is_running,
    cast(is_weekend  as BOOL)                   as is_weekend,

    -- Active context FKs
    active_order_id,
    steel_grade_active,
    hardness_index_active,
    active_maintenance_id,
    active_maintenance_type,
    last_failure_id,

    -- Demand / operational context
    demand_index,
    rush_factor,
    maintenance_deferral,
    month_pulse_index,
    trend_index,
    load_index,

    -- Sensor readings
    bearing_temp_c,
    vibration_rms,
    motor_torque_nm,
    bearing_health,
    lube_health,
    align_health,
    motor_health,

    -- Operating hours
    cumulative_op_hours,
    op_hours_since_lube,
    op_hours_since_calib,
    op_hours_since_bearing,
    op_hours_since_major,

    -- Pre-computed labels (no need to re-derive anomaly logic)
    cast(label_anomaly         as BOOL)         as is_anomaly,
    cast(label_failure_in_24h  as BOOL)         as failure_predicted_24h,
    cast(label_failure_in_72h  as BOOL)         as failure_predicted_72h,
    label_failure_type_next                     as predicted_failure_type,

    -- Z-scores for reference
    vib_norm,
    temp_norm,
    vib_z,
    temp_z,

    loaded_at

from {{ source('industrial_twin_raw', 'telemetry') }}
where timestamp is not null
  and machine_id is not null
