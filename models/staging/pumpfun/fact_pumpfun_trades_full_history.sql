{{
    config(
        materialized="table",
        snowflake_warehouse="ANALYTICS_XL",
        alias="fact_pumpfun_trades_full_history",
    )
}}

select
    block_timestamp,
    block_timestamp::date as date,
    tx_id,
    _log_id,
    swap_program,
    program_id,
    event_name,
    swapper,
    swap_from_mint,
    swap_from_symbol,
    swap_from_amount,
    swap_from_amount_usd,
    swap_to_mint,
    swap_to_symbol,
    swap_to_amount,
    swap_to_amount_usd,
    null as encoded_data,
    null as decoded_data,
    fee_recipient,
    fee_sol,
    fee_sol * p.price as fee,
    100 as fee_basis_points,
    null as creator_fee_native,
    null as creator_fee,
    null as creator_fee_basis_points,
    null as creator
from {{ ref('fact_pumpfun_trades_silver') }}
left join {{ source('SOLANA_FLIPSIDE_PRICE', 'ez_prices_hourly') }} p
        on p.hour = date_trunc('hour', block_timestamp) and p.token_address = 'So11111111111111111111111111111111111111112'


union all 

select 
    block_timestamp,
    block_timestamp::date as date,
    tx_id,
    _log_id,
    swap_program,
    program_id,
    CASE 
        WHEN swap_from_symbol = 'SOL' THEN 'buy'
        WHEN swap_to_symbol = 'SOL' THEN 'sell'
    END AS event_name,
    swapper,
    swap_from_mint,
    swap_from_symbol,
    swap_from_amount,
    swap_from_amount_usd,
    swap_to_mint,
    swap_to_symbol,
    swap_to_amount,
    swap_to_amount_usd,
    encoded_data,
    decoded_data,
    fee_recipient,
    fee_native as fee_sol,
    fee,
    fee_basis_points,
    creator_fee_native,
    creator_fee,
    creator_fee_basis_points,
    creator
from {{ ref('fact_pumpfun_trades_decoded') }}
