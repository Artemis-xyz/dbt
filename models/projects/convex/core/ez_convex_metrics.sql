{{
    config(
        materialized='table',
        snowflake_warehouse='CONVEX',
        database='CONVEX',
        schema='core',
        alias='ez_metrics'
    )
}}

with date_spine as (
    select date
    from {{ ref('dim_date_spine') }}
    where date between '2020-03-01' and to_date(sysdate())
)
, fees_and_revenue as (
    select
        date,
        sum(fees) as fees,
        sum(revenue) as revenue,
        sum(primary_supply_side_fees) as primary_supply_side_fees
    from {{ ref('fact_convex_revenue') }}
    group by 1
)
, token_incentives as (
    select
        date,
        sum(token_incentives) as token_incentives
    from {{ ref('fact_convex_token_incentives') }}
    group by 1
)
, tvl as (
    select
        date,
        sum(tvl) as tvl
    from {{ ref('fact_convex_combined_tvl') }}
    group by 1
)
, treasury_value as (
    select
        date,
        sum(usd_balance) as treasury_value
    from {{ ref('fact_convex_treasury_balance') }}
    group by 1
)
, net_treasury as (
    select
        date,
        sum(usd_balance) as net_treasury_value
    from {{ ref('fact_convex_treasury_balance') }}
    group by 1
)
, treasury_native as (
    select
        date,
        sum(native_balance) as treasury_native
    from {{ ref('fact_convex_treasury_balance') }}
    group by 1
)
, token_holders as (
    SELECT
        date,
        token_holder_count
    FROM {{ ref('fact_convex_token_holders') }}
)
, market_data as (
    {{ get_coingecko_metrics('convex-finance') }}
)

select
    date_spine.date,
    fees_and_revenue.fees,
    fees_and_revenue.revenue,
    fees_and_revenue.primary_supply_side_fees as primary_supply_side_revenue,
    fees_and_revenue.primary_supply_side_fees as total_supply_side_revenue,
    token_incentives.token_incentives,
    token_incentives.token_incentives as expenses,
    fees_and_revenue.revenue - token_incentives.token_incentives as earnings,
    tvl.tvl,
    tvl.tvl as net_deposits,
    treasury_value.treasury_value,
    net_treasury.net_treasury_value,
    treasury_native.treasury_native,
    market_data.price,
    market_data.market_cap,
    market_data.fdmc,
    market_data.token_turnover_circulating,
    market_data.token_turnover_fdv,
    market_data.token_volume,
    token_holders.token_holder_count
from date_spine
left join treasury_value using (date)
left join net_treasury using (date)
left join treasury_native using (date)
left join token_holders using (date)
left join fees_and_revenue using (date)
left join token_incentives using (date)
left join tvl using (date)
left join market_data using (date)