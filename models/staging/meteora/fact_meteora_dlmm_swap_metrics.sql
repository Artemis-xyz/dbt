{{
    config(
        materialized="incremental",
        unique_key="date",
        alias="fact_meteora_dlmm_swap_metrics",
    )
}}

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
WHERE program_id = 'LBUZKhRxPF3XUpBCjp4YzTKgLccjZhTSDM9YuVaPwxo'
AND (swap_from_mint != '8twuNzMszqWeFbDErwtf4gw13E6MUS4Hsdx5mi3aqXAM' AND swap_to_mint != '8twuNzMszqWeFbDErwtf4gw13E6MUS4Hsdx5mi3aqXAM') --filter out SB token swaps, as Solana flipside source has bad pricing data for this token
{% if is_incremental() %}
    and block_timestamp::date > (select dateadd('day', -3, max(date)) from {{ this }})
{% else %}
    and block_timestamp::date < '2025-05-05'
{% endif %}
group by 1