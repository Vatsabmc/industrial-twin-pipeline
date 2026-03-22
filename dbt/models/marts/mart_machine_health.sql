-- models/marts/mart_machine_health.sql
--
-- Daily machine health summary built from enriched telemetry.
-- Powers the machine health scorecard and trend chart in Looker Studio.
--
-- Key question answered: how is each machine's health trending over 2025,
-- and what operational pressure (demand, rush, maintenance deferral)
-- correlates with health degradation?

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

with daily as (

    select
        recording_date,
        machine_id,

        -- Reading counts
        count(*)                                as total_readings,
        countif(is_running)                     as running_readings,
        countif(is_anomaly)                     as anomaly_readings,
        countif(failure_predicted_24h)          as readings_predicting_failure_24h,

        -- Availability rate
        round(safe_divide(countif(is_running), count(*)), 4)    as availability_rate,

        -- Anomaly rate
        round(safe_divide(countif(is_anomaly), count(*)), 4)    as anomaly_rate,

        -- Average component health scores (0–1 scale in source)
        round(avg(bearing_health), 4)           as avg_bearing_health,
        round(avg(lube_health),    4)           as avg_lube_health,
        round(avg(align_health),   4)           as avg_align_health,
        round(avg(motor_health),   4)           as avg_motor_health,

        -- Composite health: equal-weight average of 4 components
        round(
            (avg(bearing_health) + avg(lube_health)
           + avg(align_health)   + avg(motor_health)) / 4,
            4
        )                                       as composite_health_score,

        -- Sensor readings
        round(avg(bearing_temp_c),  2)          as avg_bearing_temp_c,
        round(avg(vibration_rms),   4)          as avg_vibration_rms,
        round(avg(motor_torque_nm), 2)          as avg_motor_torque_nm,
        round(avg(load_index),      4)          as avg_load_index,

        -- Operating hours (take max for the day — it's cumulative)
        max(cumulative_op_hours)                as cumulative_op_hours,
        max(op_hours_since_lube)                as op_hours_since_lube,
        max(op_hours_since_bearing)             as op_hours_since_bearing,
        max(op_hours_since_major)               as op_hours_since_major,

        -- Operational pressure (daily averages)
        round(avg(demand_index),           4)   as avg_demand_index,
        round(avg(rush_factor),            4)   as avg_rush_factor,
        round(avg(maintenance_deferral),   4)   as avg_maintenance_deferral,
        round(avg(daily_expected_tons),    2)   as expected_tons_that_day,

        -- Order context
        countif(order_is_delayed)               as delayed_order_readings

    from {{ ref('int_telemetry_enriched') }}
    group by 1, 2

),

with_status as (

    select
        *,
        case
            when composite_health_score >= 0.80 then 'healthy'
            when composite_health_score >= 0.60 then 'warning'
            else                                     'critical'
        end                                     as health_status,

        -- 7-day rolling composite health
        round(
            avg(composite_health_score) over (
                partition by machine_id
                order by recording_date
                rows between 6 preceding and current row
            ), 4
        )                                       as rolling_7d_health

    from daily

)

select * from with_status