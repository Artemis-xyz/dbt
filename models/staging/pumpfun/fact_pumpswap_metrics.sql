{{
    config(
        materialized="table",
        snowflake_warehouse="ANALYTICS_XL",
        alias="fact_pumpswap_metrics",
    )
}}

with txns as (
    select
        block_timestamp::date as date,
        count(*) as spot_txns,
        count(distinct swapper) as spot_dau,
    from {{ source("SOLANA_FLIPSIDE_DEFI", "ez_dex_swaps") }}
    where program_id = 'pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA'
    group by date
)

, pumpswap_metrics as (
    select
        block_timestamp::date as date,
        sum(coalesce(swap_to_amount_usd, swap_from_amount_usd)) as spot_volume,
        sum(fee) as spot_fees,
        sum( ( lp_fee / POW(10, p.decimals)) * p.price) as spot_lp_fees,
        count(distinct creator) as spot_creators,
    from {{ ref('fact_pumpswap_trades') }} d
    left join {{ source('SOLANA_FLIPSIDE_PRICE', 'ez_prices_hourly') }} p
        on p.hour = date_trunc('hour', block_timestamp) and p.token_address = d.quote_mint
    group by date
)

select
    date,
    spot_dau,
    spot_txns,
    spot_volume,
    spot_fees,
    spot_lp_fees,
    spot_creators
from pumpswap_metrics
left join txns using(date)