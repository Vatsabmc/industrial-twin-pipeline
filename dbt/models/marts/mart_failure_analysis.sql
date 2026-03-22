-- models/marts/mart_failure_analysis.sql
--
-- One row per failure event, enriched with the machine's health state
-- and operational pressure at the time of failure.
-- Powers the failure root-cause breakdown and demand-vs-failure scatter
-- chart in Looker Studio.
--
-- Key questions answered:
--   - Which failure categories and root causes are most common?
--   - Do failures cluster when demand_index or rush_factor is high?
--   - How much maintenance deferral preceded each failure?

{{
  config(
    materialized = 'table',
    partition_by = {'field': 'occurrence_date', 'data_type': 'date', 'granularity': 'day'},
    cluster_by   = ['machine_id', 'failure_category']
  )
}}

with failures as (
    select * from {{ ref('stg_failures') }}
),

-- Aggregate machine state in the 24h window before each failure
-- by looking back in the enriched telemetry
pre_failure_state as (

    select
        f.failure_id,
        round(avg(t.bearing_health),        4) as avg_bearing_health_24h_prior,
        round(avg(t.lube_health),           4) as avg_lube_health_24h_prior,
        round(avg(t.align_health),          4) as avg_align_health_24h_prior,
        round(avg(t.motor_health),          4) as avg_motor_health_24h_prior,
        round(avg(t.maintenance_deferral),  4) as avg_maint_deferral_24h_prior,
        round(avg(t.demand_index),          4) as avg_demand_24h_prior,
        round(avg(t.rush_factor),           4) as avg_rush_24h_prior,
        countif(t.failure_predicted_24h)       as readings_that_predicted_failure

    from failures f
    join {{ ref('int_telemetry_enriched') }} t
        on  t.machine_id    = f.machine_id
        and t.recorded_at  >= timestamp_sub(f.occurred_at, interval 24 hour)
        and t.recorded_at  <  f.occurred_at
    group by 1

),

-- Was corrective maintenance performed after this failure?
post_failure_maintenance as (

    select
        m.linked_failure_id              as failure_id,
        min(m.started_at)                as first_maintenance_after,
        sum(m.duration_minutes)          as total_maint_minutes_after,
        count(*)                         as maintenance_events_after

    from {{ ref('stg_maintenance') }} m
    where m.linked_failure_id is not null
    group by 1

)

select
    f.failure_id,
    f.machine_id,
    f.occurred_at,
    f.occurrence_date,
    f.failure_category,
    f.root_cause,
    f.severity_level,
    f.steel_grade_active,
    f.is_weekend,

    -- Sensor readings at the moment of failure
    f.load_index_at_failure,
    f.bearing_temp_c_at_failure,
    f.vibration_rms_at_failure,
    f.motor_torque_nm_at_failure,
    f.bearing_health_at_failure,
    f.lube_health_at_failure,
    f.align_health_at_failure,
    f.motor_health_at_failure,

    -- Operational pressure at failure
    f.demand_index,
    f.rush_factor,
    f.maintenance_deferral          as maint_deferral_at_failure,

    -- 24h pre-failure averages
    p.avg_bearing_health_24h_prior,
    p.avg_lube_health_24h_prior,
    p.avg_align_health_24h_prior,
    p.avg_motor_health_24h_prior,
    p.avg_maint_deferral_24h_prior,
    p.avg_demand_24h_prior,
    p.avg_rush_24h_prior,
    p.readings_that_predicted_failure,

    -- Was the failure predictable? (model predicted it at least once)
    p.readings_that_predicted_failure > 0   as was_predicted,

    -- Post-failure maintenance response
    pm.first_maintenance_after,
    pm.total_maint_minutes_after,
    pm.maintenance_events_after,
    timestamp_diff(
        pm.first_maintenance_after,
        f.occurred_at,
        minute
    )                                       as minutes_to_first_maintenance

from failures f
left join pre_failure_state      p  on f.failure_id = p.failure_id
left join post_failure_maintenance pm on f.failure_id = pm.failure_id