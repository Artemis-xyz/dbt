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
        block_timestamp::date  date,
        tx_id,
        swapper trader ,
        swap_from_amount amount,
        swap_from_amount_usd amount_usd 
    FROM {{ source('SOLANA_FLIPSIDE_DEFI', 'ez_dex_swaps') }}
    WHERE swap_program ilike '%pump%'
    AND swap_from_mint ='So11111111111111111111111111111111111111112'

    UNION ALL 

    SELECT 
        block_timestamp::date  date,
        tx_id,
        swapper trader ,
        swap_to_amount amount,
        swap_to_amount_usd amount_usd 
    FROM {{ source('SOLANA_FLIPSIDE_DEFI', 'ez_dex_swaps') }}
    WHERE swap_program ilike '%pump%'
    AND swap_to_mint ='So11111111111111111111111111111111111111112'
)
SELECT * FROM all_swaps

