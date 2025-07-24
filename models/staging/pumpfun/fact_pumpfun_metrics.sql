{{
    config(
        materialized="table",
        snowflake_warehouse="ANALYTICS_XL",
        alias="fact_pumpfun_metrics",
    )
}}

with pumpfun_metrics as (
    select 
        block_timestamp::date as date,
        sum(coalesce(swap_to_amount_usd, swap_from_amount_usd)) as launchpad_volume,
        sum(fee) as launchpad_fees,
        count(distinct creator) as launchpad_creator,
    from {{ ref('fact_pumpfun_trades_full_history') }}
    where date > '2024-05-31'
    group by 1
)

, dex_swaps as (
    select
        block_timestamp::date as date,
        count(*) as launchpad_txns,
        count(distinct swapper) as launchpad_dau,
    from {{ source("SOLANA_FLIPSIDE_DEFI", "ez_dex_swaps") }}
    where program_id = '6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P'
    group by date
)

select
    date,
    launchpad_dau,
    launchpad_txns,
    launchpad_volume,
    launchpad_fees,
    launchpad_creator
from pumpfun_metrics
left join dex_swaps using(date)