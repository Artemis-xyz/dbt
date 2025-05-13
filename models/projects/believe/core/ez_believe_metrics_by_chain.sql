{{
    config(
        materialized="table",
        snowflake_warehouse="BELIEVE",
        database="believe",
        schema="core",
        alias="ez_metrics_by_chain",
    )
}}

with
    believe_swap_trades as (
        select
            date(block_timestamp) as date
            , sum(amount_usd) as trading_volume
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
    , bst.trading_volume
    , bst.txns
    , bcm.coins_minted

    -- Standardized Metrics
    , price
    , market_cap
    , fdmc
    , token_volume
    , token_turnover_circulating
    , token_turnover_fdv

    , 'solana' as chain

from believe_swap_trades bst
left join believe_coins_minted bcm
    on bst.date = bcm.date
left join market_metrics mm
    on bst.date = mm.date
