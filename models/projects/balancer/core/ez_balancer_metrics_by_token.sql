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
        sum(service_cash_flow) as primary_supply_side_revenue,
        sum(service_cash_flow_native) as primary_supply_side_revenue_native,
        sum(treasury_cash_flow + vebal_cash_flow) as revenue,
        sum(treasury_cash_flow_native + vebal_cash_flow_native) as revenue_native,

        sum(service_cash_flow) as service_fee_allocation,
        sum(service_cash_flow_native) as service_fee_allocation_native,
        sum(treasury_cash_flow) as treasury_fee_allocation,
        sum(treasury_cash_flow_native) as treasury_fee_allocation_native,
        sum(vebal_cash_flow) as staking_fee_allocation,
        sum(vebal_cash_flow_native) as staking_fee_allocation_native,

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
        sum(usd_balance) as treasury_value,
        sum(native_balance) as treasury_value_native
    FROM {{ ref('fact_balancer_treasury_by_token') }}
    where usd_balance > 0
    group by 1,2
)
, net_treasury as (
    SELECT
        date,
        token,
        sum(usd_balance) as net_treasury_usd,
        sum(native_balance) as net_treasury_native
    FROM {{ ref('fact_balancer_treasury_by_token') }}
    where token <> 'BAL'
    and usd_balance > 0
    group by 1,2
)
, treasury_native as (
    SELECT
        date,
        token,
        sum(native_balance) as treasury_native,
        sum(usd_balance) as own_token_treasury
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
    , 'balancer' as artemis_id
    , date_token_spine.token

    --Usage Data
    , coalesce(swap_metrics.unique_traders, 0) as spot_dau
    , coalesce(swap_metrics.unique_traders, 0) as dau
    , coalesce(swap_metrics.number_of_swaps, 0) as spot_txns
    , coalesce(swap_metrics.number_of_swaps, 0) as txns
    , coalesce(tvl.tvl_usd, 0) as tvl
    , coalesce(swap_metrics.trading_volume, 0) as spot_volume

    --Fee Data
    , coalesce(swap_metrics.trading_fees_native, 0) as fees_native
    , coalesce(swap_metrics.trading_fees, 0) as spot_fees
    , coalesce(swap_metrics.trading_fees, 0) as fees

    --Fee Allocation
    , coalesce(swap_metrics.service_fee_allocation, 0) as lp_fee_allocation
    , coalesce(swap_metrics.treasury_fee_allocation, 0) as foundation_fee_allocation
    , coalesce(swap_metrics.staking_fee_allocation, 0) as staking_fee_allocation

    --Financial Statements
    , coalesce(swap_metrics.revenue_native, 0) as revenue_native
    , coalesce(swap_metrics.revenue, 0) as revenue
    , coalesce(token_incentives.token_incentives, 0) as token_incentives
    , coalesce(swap_metrics.revenue, 0) - coalesce(token_incentives.token_incentives_usd, 0) as earnings

    -- Treasury Metrics
    , coalesce(treasury_by_token.treasury_value, 0) as treasury
    , coalesce(treasury_native.own_token_treasury, 0) as own_token_treasury
    , coalesce(net_treasury.net_treasury_usd, 0) as net_treasury


from date_token_spine
full outer join treasury_by_token using (date, token)
full outer join net_treasury using (date, token)
full outer join treasury_native using (date, token)
left join swap_metrics using (date, token)
left join tvl using (date, token)
left join token_incentives using (date, token)