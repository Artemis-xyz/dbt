{{
    config(
        materialized="table",
        snowflake_warehouse="SUSHISWAP_SM",
        database="sushiswap",
        schema="core",
        alias="ez_metrics_by_pool",
    )
}}

with
    trading_volume_pool as (
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
    )
select
    tvl_by_pool.date
    , 'sushiswap' as app
    , 'DeFi' as category
    , tvl_by_pool.chain
    , tvl_by_pool.version
    , tvl_by_pool.pool
    , tvl_by_pool.token_0
    , tvl_by_pool.token_0_symbol
    , tvl_by_pool.token_1
    , tvl_by_pool.token_1_symbol
    , trading_volume_pool.trading_volume
    , trading_volume_pool.trading_fees
    , trading_volume_pool.unique_traders
    , trading_volume_pool.gas_cost_native
    , trading_volume_pool.gas_cost_usd

    -- Standardized Metrics
    , tvl_by_pool.tvl
    , trading_volume_pool.trading_volume as spot_volume
    , trading_volume_pool.trading_fees as gross_protocol_revenue
    , trading_volume_pool.unique_traders as spot_dau
    , trading_volume_pool.trading_fees * 0.25 / 0.30 as service_cash_flow
    , trading_volume_pool.trading_fees * 0.05 / 0.30 as token_cash_flow
from tvl_by_pool
left join trading_volume_pool using(date, chain, version, pool)
where tvl_by_pool.date < to_date(sysdate())