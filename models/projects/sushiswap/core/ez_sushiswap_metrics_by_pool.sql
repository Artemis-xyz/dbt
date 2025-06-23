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
    , tvp.trading_volume
    , tvp.trading_fees
    , tvp.unique_traders
    , tvp.gas_cost_native
    , tvp.gas_cost_usd

    -- Standardized Metrics
    , tvp.unique_traders as spot_dau
    , tvp.trading_volume as spot_volume
    , tvl_by_pool.tvl

    -- Revenue Metrics
    , tvp.trading_fees as ecosystem_revenue
    , case
        when tvp.date between '2023-01-23' and '2024-01-23' THEN
            tvp.trading_fees * 0.0030
        else
            tvp.trading_fees * 0.0025 / 0.0030
    end as service_fee_allocation
    , case
        when tvp.date between '2023-01-23' and '2024-01-23' THEN
            0
        else
            tvp.trading_fees * 0.0005 / 0.0030
    end as staking_fee_allocation
from tvl_by_pool
left join trading_volume_pool tvp using(date, chain, version, pool)
where tvl_by_pool.date < to_date(sysdate())