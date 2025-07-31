{{
    config(
        materialized="table",
        snowflake_warehouse="TRADER_JOE",
        database="trader_joe",
        schema="core",
        alias="ez_metrics_by_chain",
    )
}}

with trading_volume_by_pool as (
    {{
        dbt_utils.union_relations(
            relations=[
                ref("fact_trader_joe_arbitrum_trading_vol_fees_traders_by_pool"),
                ref("fact_trader_joe_avalanche_trading_vol_fees_traders_by_pool"),
            ],
        )
    }}
)
, trading_volume_by_chain as (
    select
        trading_volume_by_pool.date,
        trading_volume_by_pool.chain,
        sum(coalesce(trading_volume_by_pool.trading_volume, 0)) as trading_volume,
        sum(coalesce(trading_volume_by_pool.trading_fees, 0)) as trading_fees,
        sum(coalesce(trading_volume_by_pool.unique_traders, 0)) as unique_traders,
        sum(coalesce(trading_volume_by_pool.gas_cost_native, 0)) as gas_cost_native,
        sum(coalesce(trading_volume_by_pool.gas_cost_usd, 0)) as gas_cost_usd
    from trading_volume_by_pool
    group by trading_volume_by_pool.date, trading_volume_by_pool.chain
)
, tvl_by_pool as (
    {{
        dbt_utils.union_relations(
            relations=[
                ref("fact_trader_joe_avalanche_tvl_by_pool"),
                ref("fact_trader_joe_arbitrum_tvl_by_pool"),
            ],
        )
    }}
)
, tvl_by_chain as (
    select
        tvl_by_pool.date,
        tvl_by_pool.chain,
        sum(coalesce(tvl_by_pool.tvl, 0)) as tvl
    from tvl_by_pool
    group by tvl_by_pool.date, tvl_by_pool.chain
)
, daily_txns_data as (
    select
        date(block_timestamp) as date
        , chain
        , count(*) as daily_txns
    from {{ ref("ez_trader_joe_dex_swaps")}}
    group by 1, 2
)
, token_incentives as (
    select
        date
        , 'avalanche' as chain
        , sum(coalesce(amount_usd, 0)) as token_incentives
    from {{ ref("fact_trader_joe_token_incentives") }}
    group by date
)

select
    tvl_by_chain.date
    , 'trader_joe' as artemis_id
    , tvl_by_chain.chain
    
    -- Standardized Metrics

    -- Usage Data
    , trading_volume_by_chain.unique_traders as spot_dau
    , daily_txns_data.daily_txns as spot_txns
    , trading_volume_by_chain.trading_volume as spot_volume
    , tvl_by_chain.tvl as tvl

    -- Fee Data
    , trading_volume_by_chain.trading_fees as spot_fees
    , trading_volume_by_chain.trading_fees as fees
    , token_incentives.token_incentives as token_incentives
    , trading_volume_by_chain.gas_cost_native
    , trading_volume_by_chain.gas_cost_usd as gas_cost
    
from tvl_by_chain
left join trading_volume_by_chain using(date, chain)
left join daily_txns_data using (date, chain)
left join token_incentives on tvl_by_chain.date = token_incentives.date and tvl_by_chain.chain = token_incentives.chain
where tvl_by_chain.date < to_date(sysdate())