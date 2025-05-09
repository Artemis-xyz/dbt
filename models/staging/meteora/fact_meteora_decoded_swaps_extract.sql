{{
    config(
        materialized="incremental",
        unique_key=["block_timestamp", "_log_id"],
        alias="fact_meteora_decoded_swaps_extract",
        snowflake_warehouse="METEORA",
    )
}}

with decoded_swaps_usd as (
    select
        date_trunc('day', block_timestamp) as date,
        d.*,
        decoded_data:fee::NUMBER as swap_fee_amount,
        t.symbol,
        t.price,
        (swap_fee_amount / POW(10, t.decimals) * t.price) as swap_fee_amount_usd, 
    from pc_dbt_db.prod.fact_meteora_decoded_swaps d 
    LEFT JOIN {{ source('SOLANA_FLIPSIDE_PRICE', 'ez_prices_hourly') }} t
            ON d.swap_from_mint = t.token_address
            AND date_trunc('hour', block_timestamp) = t.hour
    where swap_fee_amount_usd is not null
    and swap_fee_amount_usd < 100000
    and swap_to_mint != 'Bo9jh3wsmcC2AjakLWzNmKJ3SgtZmXEcSaW7L2FAvUsU' --SPL token edge case
    {% if is_incremental() %}
        and block_timestamp > (select dateadd('day', -3, max(block_timestamp)) from {{ this }})
    {% endif %}
) 
select * 
from decoded_swaps_usd
