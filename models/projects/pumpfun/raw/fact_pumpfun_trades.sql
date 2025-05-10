{{
    config(
        materialized='table',
        snowflake_warehouse='PUMPFUN',
        database='PUMPFUN',
        schema='raw',
        alias='fact_pumpfun_trades',
    )
 }}

with all_swaps as (
    SELECT 
        block_timestamp::date as date,
        tx_id,
        swapper as trader ,
        swap_from_amount as amount,
        swap_from_amount_usd as amount_usd_flipside,
        swap_from_amount * p.price as amount_usd_artemis 
    FROM {{ source('SOLANA_FLIPSIDE_DEFI', 'ez_dex_swaps') }} d
    LEFT JOIN {{ source('SOLANA_FLIPSIDE_PRICE', 'ez_prices_hourly') }} p
        ON p.hour = date_trunc(hour, block_timestamp)
        AND p.is_native
    WHERE swap_program ilike '%pump.fun%'
    AND swap_from_mint ='So11111111111111111111111111111111111111112'

    UNION ALL 

    SELECT 
        block_timestamp::date as date,
        tx_id,
        swapper as trader,
        swap_to_amount as amount,
        swap_to_amount_usd as amount_usd_flipside,
        swap_to_amount * p.price as amount_usd_artemis 
    FROM {{ source('SOLANA_FLIPSIDE_DEFI', 'ez_dex_swaps') }}
    LEFT JOIN {{ source('SOLANA_FLIPSIDE_PRICE', 'ez_prices_hourly') }} p
        ON p.hour = date_trunc(hour, block_timestamp)
        AND p.is_native
    WHERE swap_program ilike '%pump.fun%'
    AND swap_to_mint ='So11111111111111111111111111111111111111112'
)
SELECT * FROM all_swaps

