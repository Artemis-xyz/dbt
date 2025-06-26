{{
    config(
        materialized="table",
        snowflake_warehouse="PANCAKESWAP_SM",
        database="pancakeswap",
        schema="core",
        alias="ez_metrics_by_pool",
    )
}}

with
    trading_volume_pool as (
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
    ),
    tvl_by_pool as (
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
    ), 
    fees_revenue as (
    select
        block_timestamp::date as date
        , chain
        , version
        , pool
        , sum(trading_fees) as fees
    -- This distribution is outlined here https://docs.pancakeswap.finance/products/pancakeswap-exchange/pancakeswap-pools
        , sum(
            case
                when fee_percent = 0.0001 then trading_fees * 0.67
                when fee_percent = 0.0005 then trading_fees * 0.66
                when fee_percent = 0.0025 then trading_fees * 0.68
                when fee_percent = 0.01 then trading_fees * 0.68
            end
        ) as service_fee_allocation 
        , sum(
            case
                when fee_percent = 0.0001 then trading_fees * 0.15
                when fee_percent = 0.0005 then trading_fees * 0.15
                when fee_percent = 0.0025 then trading_fees * 0.23
                when fee_percent = 0.01 then trading_fees * 0.23 
            end
        ) as burned_fee_allocation 
        , sum(
            case
                when fee_percent = 0.0001 then trading_fees * 0.18
                when fee_percent = 0.0005 then trading_fees * 0.19
                when fee_percent = 0.0025 then trading_fees * 0.09
                when fee_percent = 0.01 then trading_fees * 0.09
            end
        ) as treasury_fee_allocation
    from {{ ref('ez_pancakeswap_dex_swaps') }}
    group by 1, 2, 3, 4
)
select
    tvl_by_pool.date
    , 'pancakeswap' as app
    , 'DeFi' as category
    , tvl_by_pool.chain
    , tvl_by_pool.version
    , tvl_by_pool.pool
    , tvl_by_pool.token_0
    , tvl_by_pool.token_0_symbol
    , tvl_by_pool.token_1
    , tvl_by_pool.token_1_symbol
    , trading_volume_pool.trading_volume
    , fees_revenue.fees
    , trading_volume_pool.unique_traders
    , trading_volume_pool.gas_cost_usd

    -- Standardized Metrics
    , tvl_by_pool.tvl
    , trading_volume_pool.unique_traders as spot_dau
    , trading_volume_pool.trading_volume as spot_volume
    , fees_revenue.fees as spot_fees
    , fees_revenue.service_fee_allocation
    , fees_revenue.burned_fee_allocation
    , fees_revenue.treasury_fee_allocation
    , fees_revenue.burned_fee_allocation + fees_revenue.treasury_fee_allocation as revenue
    -- TODO: see comment in ez_pancakeswap_metrics re: remaining fees

    , trading_volume_pool.gas_cost_native
    , trading_volume_pool.gas_cost_usd as gas_cost

from tvl_by_pool
left join trading_volume_pool using(date, chain, version, pool)
left join fees_revenue using (date, chain, version, pool)
where tvl_by_pool.date < to_date(sysdate())