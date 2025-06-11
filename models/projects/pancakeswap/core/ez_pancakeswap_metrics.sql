{{
    config(
        materialized="table",
        snowflake_warehouse="PANCAKESWAP_SM",
        database="pancakeswap",
        schema="core",
        alias="ez_metrics",
    )
}}

with trading_volume_pool as (
    {{
        dbt_utils.union_relations(
            relations=[
                ref("fact_pancakeswap_v2_arbitrum_trading_vol_fees_traders_by_pool"),
                ref("fact_pancakeswap_v2_base_trading_vol_fees_traders_by_pool"),
                ref("fact_pancakeswap_v2_bsc_trading_vol_fees_traders_by_pool"),
                ref("fact_pancakeswap_v2_ethereum_trading_vol_fees_traders_by_pool"),
                ref("fact_pancakeswap_v3_arbitrum_trading_vol_fees_traders_by_pool"),
                ref("fact_pancakeswap_v3_base_trading_vol_fees_traders_by_pool"),
                ref("fact_pancakeswap_v3_bsc_trading_vol_fees_traders_by_pool"),
                ref("fact_pancakeswap_v3_ethereum_trading_vol_fees_traders_by_pool"),
            ],
        )
    }}
)
, trading_volume as (
    select
        trading_volume_pool.date
        , sum(trading_volume_pool.trading_volume) as trading_volume
        , sum(trading_volume_pool.trading_fees) as trading_fees
        , sum(trading_volume_pool.unique_traders) as unique_traders
        , sum(trading_volume_pool.gas_cost_native) as gas_cost_native
        , sum(trading_volume_pool.gas_cost_usd) as gas_cost_usd
    from trading_volume_pool
    group by trading_volume_pool.date
)
, tvl_by_pool as (
    {{
        dbt_utils.union_relations(
            relations=[
                ref("fact_pancakeswap_v2_arbitrum_tvl_by_pool"),
                ref("fact_pancakeswap_v2_base_tvl_by_pool"),
                ref("fact_pancakeswap_v2_bsc_tvl_by_pool"),
                ref("fact_pancakeswap_v2_ethereum_tvl_by_pool"),
                ref("fact_pancakeswap_v3_arbitrum_tvl_by_pool"),
                ref("fact_pancakeswap_v3_base_tvl_by_pool"),
                ref("fact_pancakeswap_v3_bsc_tvl_by_pool"),
                ref("fact_pancakeswap_v3_ethereum_tvl_by_pool"),
            ],
        )
    }}
)
, tvl as (
    select
        tvl_by_pool.date
        , sum(tvl_by_pool.tvl) as tvl
    from tvl_by_pool
    group by tvl_by_pool.date
)
, token_incentives as (
    select
        date
        , sum(amount_usd) as token_incentives_usd
    from {{ ref('fact_pancakeswap_token_incentives') }}
    group by date
)
, fees_revenue as (
    select
        block_timestamp::date as date
        , sum(trading_fees) as fees
    -- This distribution is outlined here https://docs.pancakeswap.finance/products/pancakeswap-exchange/pancakeswap-pools
        , sum(
            case
                when fee_percent = 0.0001 then trading_fees * 0.67
                when fee_percent = 0.0005 then trading_fees * 0.66
                when fee_percent = 0.0025 then trading_fees * 0.68
                when fee_percent = 0.01 then trading_fees * 0.68
            end
        ) as service_cash_flow 
        , sum(
            case
                when fee_percent = 0.0001 then trading_fees * 0.15
                when fee_percent = 0.0005 then trading_fees * 0.15
                when fee_percent = 0.0025 then trading_fees * 0.23
                when fee_percent = 0.01 then trading_fees * 0.23 
            end
        ) as burned_cash_flow 
        , sum(
            case
                when fee_percent = 0.0001 then trading_fees * 0.18
                when fee_percent = 0.0005 then trading_fees * 0.19
                when fee_percent = 0.0025 then trading_fees * 0.09
                when fee_percent = 0.01 then trading_fees * 0.09
            end
        ) as treasury_cash_flow
    from {{ ref('ez_pancakeswap_dex_swaps') }}
    group by date
)
select
    tvl.date
    , 'pancakeswap' as app
    , 'DeFi' as category
    , tvl.tvl
    
    , trading_volume.trading_volume as spot_volume
    , trading_volume.unique_traders as spot_dau
    
    , fees_revenue.fees as spot_fees
    , fees_revenue.fees as fees
    -- About 68% of fees go to LPs
    , fees_revenue.service_cash_flow as service_fee_allocation
    , fees_revenue.burned_cash_flow as burned_fee_allocation
    , fees_revenue.treasury_cash_flow as treasury_fee_allocation
    -- TODO: the remaining 32% of fees are distributed differently depending on the fee tier of the pool. We currently have the fee tier in
    -- pancakeswap's ez_dex_swap. This needs to be pulled forward to the correct tables.
    -- The remaining fees are distributed among CAKE burns, Treasury, and Fixed Term CAKE Stakers
    -- https://docs.pancakeswap.finance/products/pancakeswap-exchange/pancakeswap-pools
    
    , token_incentives.token_incentives_usd as token_incentives
    , trading_volume.gas_cost_native as gas_cost_native
    , trading_volume.gas_cost_usd as gas_cost
from tvl
left join trading_volume using(date)
left join token_incentives using(date)
left join fees_revenue using(date)
where tvl.date < to_date(sysdate())