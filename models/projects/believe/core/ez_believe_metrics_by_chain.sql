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
            , count(distinct trader) as unique_traders
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
    
    , believe_fees as (
        select
            date
            , sum(amount_native) as fees_native
            , sum(amount_usd) as ecosystem_revenue
        from {{ ref('fact_believe_fees') }}
        group by 1
    )

    , market_metrics as (
        {{  get_coingecko_metrics('ben-pasternak')  }}
    )

select
    bst.date
    , 'solana' as chain
    
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
    , bf.ecosystem_revenue
    , bf.fees_native
    , bf.ecosystem_revenue * 0.5 as foundation_fee_allocation
    , bf.ecosystem_revenue * 0.5 as service_fee_allocation

from believe_swap_trades bst
left join believe_coins_minted bcm
    on bst.date = bcm.date
left join market_metrics mm
    on bst.date = mm.date
left join believe_fees bf
    on bst.date = bf.date
