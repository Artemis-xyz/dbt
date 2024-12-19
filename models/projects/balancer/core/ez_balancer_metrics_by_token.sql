{{
    config(
        materialized='table',
        snowflake_warehouse='BALANCER',
        database='BALANCER',
        schema='core',
        alias='ez_metrics_by_token'
    )
}}

with date_spine as (
    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="2020-03-01",
        end_date="current_date()"
    ) }}
)

, all_tvl_by_token as (
    SELECT
        date,
        symbol,
        sum(tvl_native) as tvl_native,
        sum(tvl_usd) as tvl_usd
    FROM {{ ref('fact_balancer_tvl_by_chain_and_token') }}
    group by 1,2
)

select
    date_spine.date,
    all_tvl_by_token.symbol,
    all_tvl_by_token.tvl_native,
    all_tvl_by_token.tvl_usd
from date_spine
full outer join all_tvl_by_token on date_spine.date = all_tvl_by_token.date
