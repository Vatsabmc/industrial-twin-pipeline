-- models/staging/stg_orders.sql
{{ config(materialized='view') }}

select
    order_id,
    machine_id_started                           as machine_id,
    market,
    steel_grade,
    hardness_index,
    total_weight_tons,
    status,
    is_completed,

    timestamp_planned                            as planned_at,
    timestamp_start                              as started_at,
    timestamp_end                                as ended_at,
    DATE(timestamp_planned)         as planned_date,
    DATE(timestamp_start)              as start_date,

    duration_minutes_planned,
    actual_duration_minutes,
    duration_variance_minutes,

    -- Positive variance = late, negative = early.
    -- False for incomplete orders (null variance) — they are not delayed, just unfinished.
    coalesce(duration_variance_minutes > 0, false) as is_delayed,

    demand_index_at_intake,
    rush_factor_at_intake,
    month_pulse_index_at_intake,
    trend_index_at_intake,
    cast(is_weekend_intake as bool)              as is_weekend_intake,

    loaded_at

from {{ source('industrial_twin_raw', 'orders') }}
where order_id is not null  