--Pumpfun trades with fee as .01 * swap_amount_sol and fee_recipient extracted from buy/sell event logs
-- From pumpfun inception to may 11 2025, when pumpfun IDL changed to log richer fee information
{{
    config(
        materialized="incremental",
        snowflake_warehouse="ANALYTICS_XL",
        alias="fact_pumpfun_trades_silver",
         unique_key=['ez_swaps_id'],
    )
}}

-- Extract the fee recipient account from the decoded logs for each the buy & the sell event
{% if not is_incremental() %}
    with fee_recipients as (
        select
            *,
            ROW_NUMBER() OVER (PARTITION BY tx_id ORDER BY (index)) as event_number,
            CONCAT(tx_id,'-',event_number, '-pump.fun') AS log_id,
            event_type as event_name,
            decoded_instruction:accounts[1]['pubkey']::STRING as fee_recipient
        from {{ source('SOLANA_FLIPSIDE', 'fact_decoded_instructions') }}
        where program_id = '6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P'
        and ((event_type = 'buy') or (event_type = 'sell'))
        and block_timestamp::date < '2025-05-12'
    )

    -- Join the fee_address into the ez_dex_swaps, and compute approximate fee information
    , trade_with_fee_recipient_2 as (
        select
            dex.*, 
            f.fee_recipient, 
            event_name,
            CASE 
                WHEN swap_from_symbol = 'SOL' THEN dex.swap_from_amount * .01
                WHEN swap_to_symbol = 'SOL' THEN dex.swap_to_amount * .01
            END AS fee_sol
        from {{ source('SOLANA_FLIPSIDE_DEFI', 'ez_dex_swaps') }} dex
        left join fee_recipients f on dex._log_id = f.log_id
        where dex.program_id = '6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P'
        and dex.block_timestamp::date < '2025-05-12'
    ) select * from trade_with_fee_recipient_2

{% else %}
    select * from {{ this }}
{% endif %}
