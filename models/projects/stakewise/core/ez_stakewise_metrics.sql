{{
    config(
        materialized="table",
        snowflake_warehouse="STAKEWISE",
        database="stakewise",
        schema="core",
        alias="ez_metrics",
    )
}}

with
    staked_eth_metrics as (
        select
            date,
            'ethereum' as chain,
            num_staked_eth,
            amount_staked_usd,
            num_staked_eth_net_change,
            amount_staked_usd_net_change
        from {{ ref('fact_stakewise_staked_eth_count_with_USD_and_change') }}
    ),
    market_data as ({{ get_coingecko_metrics('stakewise') }})
select
    staked_eth_metrics.date
    , 'stakewise' as artemis_id

    --Market Data
    , market_data.price
    , market_data.market_cap
    , market_data.fdmc
    , market_data.token_volume

    --Usage Data
    , staked_eth_metrics.num_staked_eth as tvl_native
    , staked_eth_metrics.amount_staked_usd as tvl

    --Other Data
    , market_data.token_turnover_fdv
    , market_data.token_turnover_circulating

    
from staked_eth_metrics
left join market_data using (date)
where staked_eth_metrics.date < to_date(sysdate())
