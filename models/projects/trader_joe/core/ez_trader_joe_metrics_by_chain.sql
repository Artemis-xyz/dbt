{{
    config(
        materialized="table",
        snowflake_warehouse="TRADER_JOE",
        database="trader_joe",
        schema="core",
        alias="ez_metrics_by_chain",
    )
}}

with
    trading_volume_by_pool as (
       {{
            dbt_utils.union_relations(
                relations=[
                    ref("fact_trader_joe_arbitrum_trading_vol_fees_traders_by_pool"),
                    ref("fact_trader_joe_avalanche_trading_vol_fees_traders_by_pool"),
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
                    ref("fact_trader_joe_avalanche_tvl_by_pool"),
                    ref("fact_trader_joe_arbitrum_tvl_by_pool"),
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
    ),
    daily_txns_data as (
        select
            date(block_timestamp) as date
            , chain
            , count(*) as daily_txns
        from {{ ref("ez_trader_joe_dex_swaps")}}
        group by 1, 2
    )
    , v2_data as (
        select
            date,
            chain,
            sum(total_volume) as total_volume,
            sum(total_fees) as total_fees,
            sum(protocol_fees) as protocol_fees,
            sum(unique_traders) as unique_traders,
            sum(daily_txns) as daily_txns
        from {{ ref("fact_trader_joe_v2_all_versions_metrics")}}
        group by 1, 2
    )
select
    tvl_by_chain.date,
    'trader_joe' as app,
    'DeFi' as category,
    tvl_by_chain.chain,
    tvl_by_chain.tvl,
    coalesce(trading_volume_by_chain.trading_volume, 0) + coalesce(v2_data.total_volume, 0) as trading_volume,
    coalesce(trading_volume_by_chain.trading_fees, 0) + coalesce(v2_data.total_fees, 0) as trading_fees,
    coalesce(v2_data.protocol_fees, 0) as revenue,
    coalesce(trading_volume_by_chain.unique_traders, 0) + coalesce(v2_data.unique_traders, 0) as unique_traders,
    trading_volume_by_chain.gas_cost_native,
    trading_volume_by_chain.gas_cost_usd,
    coalesce(daily_txns_data.daily_txns, 0) + coalesce(v2_data.daily_txns, 0) as txns
from tvl_by_chain
left join trading_volume_by_chain using(date, chain)
left join daily_txns_data using (date, chain)
left join v2_data using (date, chain)
where tvl_by_chain.date < to_date(sysdate())
