{{
    config(
        materialized="table",
        snowflake_warehouse="PANCAKESWAP_SM",
        database="pancakeswap",
        schema="core",
        alias="ez_metrics_by_chain",
    )
}}

with trading_volume_by_pool as (
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
, trading_volume_by_chain as (
    select
        trading_volume_by_pool.date,
        trading_volume_by_pool.chain,
        sum(trading_volume_by_pool.trading_volume) as trading_volume,
        sum(trading_volume_by_pool.trading_fees) as trading_fees,
        sum(trading_volume_by_pool.unique_traders) as unique_traders,
        sum(trading_volume_by_pool.gas_cost_native) as gas_cost_native,
        sum(trading_volume_by_pool.gas_cost_usd) as gas_cost_usd
    from trading_volume_by_pool
    group by trading_volume_by_pool.date, trading_volume_by_pool.chain
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
, tvl_by_chain as (
    select
        tvl_by_pool.date,
        tvl_by_pool.chain,
        sum(tvl_by_pool.tvl) as tvl
    from tvl_by_pool
    group by tvl_by_pool.date, tvl_by_pool.chain
)
, token_incentives as (
    select
        date,
        'bsc' as chain,
        sum(amount_usd) as token_incentives_usd
    from {{ ref('fact_pancakeswap_token_incentives') }}
    where chain = 'bsc'
    group by date
)
, fees_revenue as (
    select
        block_timestamp::date as date
        , chain
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
    group by 1, 2
)
select
    tvl_by_chain.date
    , 'pancakeswap' as app
    , 'DeFi' as category
    , tvl_by_chain.chain
    , tvl_by_chain.tvl
    , trading_volume_by_chain.trading_volume    
    , trading_volume_by_chain.trading_fees
    , trading_volume_by_chain.unique_traders
    , trading_volume_by_chain.gas_cost_usd

    -- Standardized Metrics

    -- Usage Metrics
    , trading_volume_by_chain.unique_traders as spot_dau
    , trading_volume_by_chain.trading_volume as spot_volume
    , trading_volume_by_chain.gas_cost_usd as gas_cost
    , trading_volume_by_chain.gas_cost_native

    -- Cashflow Metrics
    , fees_revenue.fees as spot_fees
    , fees_revenue.fees as fees
    , fees_revenue.service_cash_flow as service_cash_flow
    , fees_revenue.burned_cash_flow as burned_cash_flow
    , fees_revenue.treasury_cash_flow as treasury_cash_flow
    , token_incentives.token_incentives_usd as token_incentives

from tvl_by_chain
left join trading_volume_by_chain using(date, chain)
left join token_incentives using(date, chain)
left join fees_revenue using(date, chain)
where tvl_by_chain.date < to_date(sysdate())