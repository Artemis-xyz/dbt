{{
    config(
        materialized="table",
        snowflake_warehouse="PANCAKESWAP_SM",
        database="pancakeswap",
        schema="core",
        alias="ez_metrics_by_chain",
    )
}}

with
    trading_volume_by_pool as (
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
    trading_volume_by_chain as (
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
    tvl_by_chain as (
        select
            tvl_by_pool.date,
            tvl_by_pool.chain,
            sum(tvl_by_pool.tvl) as tvl
        from tvl_by_pool
        group by tvl_by_pool.date, tvl_by_pool.chain
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
    , NULL AS token_incentives
    -- Standardized Metrics
    , trading_volume_by_chain.unique_traders as spot_dau
    , trading_volume_by_chain.trading_volume as spot_volume
    , trading_volume_by_chain.trading_fees as spot_fees
    , trading_volume_by_chain.trading_fees as gross_protocol_revenue
    , trading_volume_by_chain.trading_fees * .68 as service_cash_flow
    -- TODO: see comment in ez_pancakeswap_metrics re: remaining fees

    , trading_volume_by_chain.gas_cost_usd as gas_cost
    , trading_volume_by_chain.gas_cost_native

from tvl_by_chain
left join trading_volume_by_chain using(date, chain)
where tvl_by_chain.date < to_date(sysdate())