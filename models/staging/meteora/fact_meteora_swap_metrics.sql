{{
    config(
        materialized="table",
        snowflake_warehouse="ANALYTICS_XL",
        alias="fact_meteora_swap_metrics",
    )
}}

with swap_metrics as (
    SELECT
        block_timestamp::date as date,
        count(distinct swapper) as unique_traders, 
        count(distinct tx_id) as number_of_swaps,
        sum(swap_from_amount_usd) as amount_in_usd,
        sum(swap_to_amount_usd) as amount_out_usd,
        count(
            distinct concat(swap_to_symbol,'-',swap_from_symbol)
        ) as pairs_traded,
        sum(coalesce(swap_to_amount_usd, swap_from_amount_usd)) as trading_volume 
    FROM {{ source('SOLANA_FLIPSIDE_DEFI', 'ez_dex_swaps') }}
    WHERE swap_program ilike '%meteora%'
    AND (swap_from_mint != '8twuNzMszqWeFbDErwtf4gw13E6MUS4Hsdx5mi3aqXAM' AND swap_to_mint != '8twuNzMszqWeFbDErwtf4gw13E6MUS4Hsdx5mi3aqXAM') --filter out SB token swaps, as Solana flipside source has bad pricing data for this token
    group by 1
) select * from swap_metrics