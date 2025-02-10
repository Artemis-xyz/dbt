{{
    config(
        materialized='table',
        snowflake_warehouse='BALANCER',
        database='BALANCER',
        schema='core',
        alias='ez_metrics_by_token'
    )
}}

with swap_metrics as (
    SELECT
        block_timestamp::date as date,
        token_in_symbol as token,
        count(*) as number_of_swaps,
        count(distinct sender) as unique_traders,
        sum(trading_volume) as trading_volume,
        sum(trading_volume_native) as trading_volume_native,
        sum(fee_usd) as trading_fees,
        sum(fee_native) as trading_fees_native,
        sum(supply_side_revenue_usd) as primary_supply_side_revenue,
        sum(supply_side_revenue_native) as primary_supply_side_revenue_native,
        sum(revenue) as revenue,
        sum(revenue_native) as revenue_native
    FROM {{ ref('ez_balancer_dex_swaps') }}
    group by 1,2
)
, tvl as (
    SELECT
        date,
        token,
        sum(token_balance) as tvl_native,
        sum(amount_usd) as tvl_usd
    FROM {{ ref('fact_balancer_tvl_by_chain_and_token') }}
    where amount_usd > 0
    group by 1,2
)
, treasury_by_token as (
    SELECT
        date,
        token,
        sum(usd_balance) as usd_balance
    FROM {{ ref('fact_balancer_treasury_by_token') }}
    where usd_balance > 0
    group by 1,2
)
, net_treasury as (
    SELECT
        date,
        token,
        sum(usd_balance) as net_treasury_usd
    FROM {{ ref('fact_balancer_treasury_by_token') }}
    where token <> 'BAL'
    and usd_balance > 0
    group by 1,2
)
, treasury_native as (
    SELECT
        date,
        token,
        sum(native_balance) as treasury_native
    FROM {{ ref('fact_balancer_treasury_by_token') }}
    where token = 'BAL'
    and native_balance > 0
    group by 1,2
)
, token_incentives as (
    SELECT
        date,
        token,
        sum(amount_usd) as token_incentives,
        sum(amount_native) as token_incentives_native
    FROM {{ ref('fact_balancer_token_incentives') }}
    group by 1,2
)
,date_token_spine as (
    SELECT
        distinct
        date,
        token
    from {{ ref('dim_date_spine') }}
    CROSS JOIN (
                SELECT distinct token from treasury_by_token
                UNION
                SELECT distinct token from net_treasury
                UNION
                SELECT distinct token from treasury_native
                UNION
                SELECT distinct token from tvl
                )
    where date between '2020-03-01' and to_date(sysdate())
)
select
    date_token_spine.date
    , date_token_spine.token
    , swap_metrics.number_of_swaps
    , swap_metrics.unique_traders
    , swap_metrics.trading_volume
    , swap_metrics.trading_volume_native
    , swap_metrics.trading_fees
    , swap_metrics.trading_fees_native
    , swap_metrics.trading_fees as fees
    , swap_metrics.trading_fees_native as fees_native
    , swap_metrics.primary_supply_side_revenue
    , swap_metrics.primary_supply_side_revenue_native
    , swap_metrics.revenue
    , swap_metrics.revenue_native
    , token_incentives.token_incentives
    , token_incentives.token_incentives_native
    , tvl.tvl_usd
    , tvl.tvl_usd as net_deposits
    , tvl.tvl_native
    , tvl.tvl_native as net_deposits_native
    , treasury_by_token.usd_balance as treasury_value
    , net_treasury.net_treasury_usd as net_treasury_value
    , treasury_native.treasury_native as treasury_native
from date_token_spine
full outer join treasury_by_token using (date, token)
full outer join net_treasury using (date, token)
full outer join treasury_native using (date, token)
left join swap_metrics using (date, token)
left join tvl using (date, token)
left join token_incentives using (date, token)