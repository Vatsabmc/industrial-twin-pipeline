-- models/staging/stg_demand.sql
{{ config(materialized='view') }}

-- Wraps source in a subquery to rename the reserved word 'date'
with source as (
    select * from {{ source('industrial_twin_raw', 'demand') }}
)

select
    `date`                            as demand_date,
    weekday,
    cast(is_weekend as BOOL)        as is_weekend,
    trend_index,
    seasonal_index,
    month_pulse_index,
    expected_total_tons,
    market_split_domestic,
    market_split_export,
    expected_domestic_tons,
    expected_export_tons,
    loaded_at

from source
where `date` is not null