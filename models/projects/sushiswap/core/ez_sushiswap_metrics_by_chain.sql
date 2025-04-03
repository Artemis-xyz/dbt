{{
    config(
        materialized="table",
        snowflake_warehouse="SUSHISWAP_SM",
        database="sushiswap",
        schema="core",
        alias="ez_metrics_by_chain",
    )
}}

with
    trading_volume_by_pool as (
       {{
            dbt_utils.union_relations(
                relations=[
                    ref("fact_sushiswap_v2_arbitrum_trading_vol_fees_traders_by_pool"),
                    ref("fact_sushiswap_v2_avalanche_trading_vol_fees_traders_by_pool"),
                    ref("fact_sushiswap_v2_bsc_trading_vol_fees_traders_by_pool"),
                    ref("fact_sushiswap_v2_ethereum_trading_vol_fees_traders_by_pool"),
                    ref("fact_sushiswap_v2_gnosis_trading_vol_fees_traders_by_pool"),
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
                    ref("fact_sushiswap_v2_gnosis_tvl_by_pool"),
                    ref("fact_sushiswap_v2_ethereum_tvl_by_pool"),
                    ref("fact_sushiswap_v2_bsc_tvl_by_pool"),
                    ref("fact_sushiswap_v2_avalanche_tvl_by_pool"),
                    ref("fact_sushiswap_v2_arbitrum_tvl_by_pool"),

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
    , 'sushiswap' as app
    , 'DeFi' as category
    , tvl_by_chain.chain
    , trading_volume_by_chain.trading_volume
    , trading_volume_by_chain.trading_fees
    , trading_volume_by_chain.unique_traders
    , trading_volume_by_chain.gas_cost_native
    , trading_volume_by_chain.gas_cost_usd

    -- Standardized Metrics
    , trading_volume_by_chain.unique_traders as spot_dau
    , trading_volume_by_chain.trading_volume as spot_volume
    , tvl_by_chain.tvl

    -- Revenue Metrics
    , trading_volume_by_chain.trading_fees as gross_protocol_revenue
    , trading_volume_by_chain.trading_fees * 0.25 / 0.30 as service_cash_flow
    , trading_volume_by_chain.trading_fees * 0.05 / 0.30 as token_cash_flow
from tvl_by_chain
left join trading_volume_by_chain using(date, chain)
where tvl_by_chain.date < to_date(sysdate())