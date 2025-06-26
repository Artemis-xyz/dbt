{{
    config(
        materialized="table",
        snowflake_warehouse="STADER",
        database="stader",
        schema="core",
        alias="ez_metrics",
    )
}}

with
    staked_eth_metrics as (
        select
            date,
            chain,
            num_staked_eth,
            amount_staked_usd,
            num_staked_eth_net_change,
            amount_staked_usd_net_change
        from {{ ref('fact_ethx_staked_eth_count_with_usd_and_change') }}
    )
    , market_metrics as (
        {{ get_coingecko_metrics('stader') }}
    )
    , date_spine as (
        select
            date
        from {{ ref('dim_date_spine') }}
        where date between (
                SELECT min(date) FROM (
                    SELECT date FROM staked_eth_metrics
                    UNION ALL
                    SELECT date FROM market_metrics
                )
            ) and to_date(sysdate())
    )
select
    date_spine.date,
    'stader' as app,
    'DeFi' as category,

    --Old metrics needed for compatibility
    staked_eth_metrics.num_staked_eth,
    staked_eth_metrics.amount_staked_usd,
    staked_eth_metrics.num_staked_eth_net_change,
    staked_eth_metrics.amount_staked_usd_net_change

    --Market Metrics
    , market_metrics.price as price
    , market_metrics.token_volume as token_volume
    , market_metrics.market_cap as market_cap
    , market_metrics.fdmc as fdmc

    --Standardized Metrics
    , staked_eth_metrics.num_staked_eth as tvl_native
    , staked_eth_metrics.num_staked_eth as lst_tvl_native
    , staked_eth_metrics.amount_staked_usd as tvl
    , staked_eth_metrics.amount_staked_usd as lst_tvl
    , staked_eth_metrics.num_staked_eth_net_change as lst_tvl_native_net_change
    , staked_eth_metrics.amount_staked_usd_net_change as lst_tvl_net_change

    --Other Metrics
    , market_metrics.token_turnover_circulating
    , market_metrics.token_turnover_fdv

from date_spine
left join staked_eth_metrics using (date)
left join market_metrics using (date)
where date_spine.date < to_date(sysdate())
