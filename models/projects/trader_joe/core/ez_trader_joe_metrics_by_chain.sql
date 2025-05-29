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
        sum(tvl_by_pool.tvl) as tvl
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
        , sum(amount_usd) as token_incentives
    from {{ ref("fact_trader_joe_token_incentives") }}
    group by date
)

select
    tvl_by_chain.date
    , 'trader_joe' as app
    , 'DeFi' as category
    , tvl_by_chain.chain

    --Old metrics needed for compatibility
    , trading_volume_by_chain.trading_volume
    , trading_volume_by_chain.trading_fees
    , trading_volume_by_chain.unique_traders
    , trading_volume_by_chain.gas_cost_usd
    , daily_txns_data.daily_txns as number_of_swaps
    

    -- Standardized Metrics

    -- Usage Metrics
    , trading_volume_by_chain.unique_traders as spot_dau
    , daily_txns_data.daily_txns as spot_txns
    , trading_volume_by_chain.trading_volume as spot_volume
    , tvl_by_chain.tvl as tvl

    -- Cashflow Metrics
    , trading_volume_by_chain.trading_fees as spot_fees
    , trading_volume_by_chain.trading_fees as ecosystem_revenue
    , token_incentives.token_incentives as token_incentives
    , trading_volume_by_chain.gas_cost_native
    , trading_volume_by_chain.gas_cost_usd as gas_cost
    
from tvl_by_chain
left join trading_volume_by_chain using(date, chain)
left join daily_txns_data using (date, chain)
left join token_incentives on tvl_by_chain.date = token_incentives.date and tvl_by_chain.chain = token_incentives.chain
where tvl_by_chain.date < to_date(sysdate())