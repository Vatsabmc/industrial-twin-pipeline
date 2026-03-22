-- models/intermediate/int_telemetry_enriched.sql
--
-- Central enriched telemetry table. Joins the four context tables
-- onto telemetry using the foreign keys already present in the data.
-- This is the foundation for all three marts.
--
-- Grain: one row per telemetry reading (262,800 rows)

{{
  config(
    materialized = 'table',
    partition_by = {
      'field':       'recording_date',
      'data_type':   'date',
      'granularity': 'day'
    },
    cluster_by = ['machine_id']
  )
}}

with telemetry as (
    select * from {{ ref('stg_telemetry') }}
),

orders as (
    select
        order_id,
        market,
        steel_grade,
        total_weight_tons,
        is_completed,
        is_delayed,
        duration_variance_minutes,
        actual_duration_minutes,
        duration_minutes_planned
    from {{ ref('stg_orders') }}
),

failures as (
    select
        failure_id,
        failure_category,
        root_cause,
        severity_level
    from {{ ref('stg_failures') }}
),

maintenance as (
    select
        maintenance_id,
        maint_type,
        is_planned,
        duration_minutes            as maint_duration_minutes
    from {{ ref('stg_maintenance') }}
),

demand as (
    select
        demand_date,
        expected_total_tons,
        seasonal_index
    from {{ ref('stg_demand') }}

)

select
    -- Telemetry core
    t.recorded_at,
    t.recording_date,
    t.machine_id,
    t.shift_id,
    t.status,
    t.is_running,
    t.is_weekend,
    t.is_anomaly,
    t.failure_predicted_24h,
    t.failure_predicted_72h,
    t.predicted_failure_type,

    -- Sensor readings
    t.bearing_temp_c,
    t.vibration_rms,
    t.motor_torque_nm,
    t.load_index,
    t.bearing_health,
    t.lube_health,
    t.align_health,
    t.motor_health,

    -- Operating hours
    t.cumulative_op_hours,
    t.op_hours_since_lube,
    t.op_hours_since_calib,
    t.op_hours_since_bearing,
    t.op_hours_since_major,

    -- Operational pressure
    t.demand_index,
    t.rush_factor,
    t.maintenance_deferral,

    -- Active order context (left join — may be null when idle)
    t.active_order_id,
    o.market                        as order_market,
    o.steel_grade                   as order_steel_grade,
    o.total_weight_tons             as order_weight_tons,
    o.is_completed                  as order_is_completed,
    o.is_delayed                    as order_is_delayed,
    o.duration_variance_minutes     as order_duration_variance_min,

    -- Last failure context (left join — null until first failure occurs)
    t.last_failure_id,
    f.failure_category,
    f.root_cause                    as failure_root_cause,
    f.severity_level                as failure_severity,

    -- Active maintenance context (left join — null when not in maintenance)
    t.active_maintenance_id,
    t.active_maintenance_type,
    m.is_planned                    as maint_is_planned,
    m.maint_duration_minutes,

    -- Daily demand context
    d.expected_total_tons           as daily_expected_tons,
    d.seasonal_index                as daily_seasonal_index

from telemetry t
left join orders      o on t.active_order_id       = o.order_id
left join failures    f on t.last_failure_id        = f.failure_id
left join maintenance m on t.active_maintenance_id  = m.maintenance_id
left join demand      d on t.recording_date         = d.demand_date
