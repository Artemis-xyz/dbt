{{
    config(
        materialized="table",
        snowflake_warehouse="BELIEVE",
        database="believe",
        schema="core",
        alias="ez_metrics",
    )
}}

with
    believe_swap_trades as (
        select
            date(block_timestamp) as date
            , sum(amount_usd) as trading_volume
            , count(distinct trader) as unique_traders
            , count(distinct tx_id) as txns
        from {{ ref('fact_believe_trades') }}
        group by 1
    )

    , believe_coins_minted as (
        select
            date(block_timestamp) as date
            , count(distinct coins_minted_address) as coins_minted
        from {{ ref('fact_believe_coins_minted') }}
        group by 1
    )

    , market_metrics as (
        {{  get_coingecko_metrics('ben-pasternak')  }}
    )

select
    bst.date

    -- Standardized Metrics

    -- Market Metrics
    , price
    , market_cap
    , fdmc
    , token_volume
    , token_turnover_circulating
    , token_turnover_fdv

    -- Usage Metrics
    , bst.trading_volume as launchpad_volumes
    , bst.txns as launchpad_txns
    , bst.unique_traders as launchpad_dau
    , bcm.coins_minted

from believe_swap_trades bst
left join believe_coins_minted bcm
    on bst.date = bcm.date
left join market_metrics mm
    on bst.date = mm.date
