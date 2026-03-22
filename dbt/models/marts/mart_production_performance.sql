-- models/marts/mart_production_performance.sql
--
-- Monthly production performance: planned vs actual output,
-- order completion rates, and schedule adherence by machine and market.
-- Powers the production performance tile in Looker Studio.
--
-- Key questions answered:
--   - Are machines completing orders on time?
--   - Does high demand or rush factor correlate with delays?
--   - Which steel grades or markets cause the most delays?

{{
  config(
    materialized = 'table',
    partition_by = {'field': 'month_date', 'data_type': 'date', 'granularity': 'day'},
    cluster_by   = ['machine_id', 'market']
  )
}}

with orders as (
    select * from {{ ref('stg_orders') }}
),

demand as (
    select * from {{ ref('stg_demand') }}
),

monthly_orders as (

    select
        date_trunc(planned_date, month)          as month_date,
        machine_id,
        market,
        steel_grade,

        count(*)                                 as total_orders,
        countif(is_completed)                    as completed_orders,
        countif(is_delayed)                      as delayed_orders,
        countif(not is_completed)                as incomplete_orders,

        round(safe_divide(
            countif(is_completed), count(*)), 4) as completion_rate,

        round(safe_divide(
            countif(is_delayed), countif(is_completed)), 4)
                                                 as delay_rate,

        round(sum(total_weight_tons), 2)         as total_tons_produced,
        round(avg(total_weight_tons), 2)         as avg_order_weight_tons,

        round(avg(duration_variance_minutes), 1) as avg_duration_variance_min,
        round(avg(demand_index_at_intake),    4) as avg_demand_index,
        round(avg(rush_factor_at_intake),     4) as avg_rush_factor

    from orders
    where machine_id is not null
    group by 1, 2, 3, 4

),

monthly_demand as (

    select
        date_trunc(demand_date, month)         as month_date,
        round(sum(expected_total_tons), 2)   as monthly_expected_tons,
        round(avg(trend_index), 4)           as avg_trend_index,
        round(avg(seasonal_index), 4)        as avg_seasonal_index

    from demand
    group by 1

)

select
    o.month_date,
    o.machine_id,
    o.market,
    o.steel_grade,

    o.total_orders,
    o.completed_orders,
    o.delayed_orders,
    o.incomplete_orders,
    o.completion_rate,
    o.delay_rate,
    o.total_tons_produced,
    o.avg_order_weight_tons,
    o.avg_duration_variance_min,
    o.avg_demand_index,
    o.avg_rush_factor,

    d.monthly_expected_tons,
    d.avg_trend_index,
    d.avg_seasonal_index,

    -- Actual vs expected production ratio
    round(safe_divide(
        o.total_tons_produced,
        d.monthly_expected_tons
    ), 4)                                    as production_vs_forecast_ratio

from monthly_orders o
left join monthly_demand d using (month_date)