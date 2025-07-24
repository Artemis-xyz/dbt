{{
    config(
        materialized="incremental",
        snowflake_warehouse="ANALYTICS_XL",
        alias="fact_pumpswap_trades_encoded",
        unique_key=['ez_swaps_id'],
    )
}}

--Fetch sell/buy events from pumpswap in the last three days from the latest entry in the current fact_pumpswap_trades_encoded table
with quote_mints_by_trade as (
    select
        *,
        decoded_instruction:accounts[4]['pubkey']::STRING as quote_mint,
        CASE 
            WHEN inner_index is null THEN ROW_NUMBER() OVER (PARTITION BY tx_id ORDER BY (index * 1000))
            WHEN inner_index is not null THEN ROW_NUMBER() OVER (PARTITION BY tx_id ORDER BY (index * 1000 + inner_index))
        END AS event_number,
        CONCAT(tx_id,'-',event_number, '-pumpswap') AS log_id,
        event_type as event_name,
    from {{ source('SOLANA_FLIPSIDE', 'fact_decoded_instructions') }} 
    where program_id = 'pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA'
    and event_type IN ('sell', 'buy')
    and block_timestamp::date > '2025-03-19'
    {% if is_incremental() %}
        and block_timestamp::date > (select dateadd('day', -3, max(block_timestamp::date)) from {{ this }})
    {% endif %}
)

--join into ez_dex_swaps transactions from the last three days from the latest entry in the current fact_pumpswap_trades_encoded table
--, resulting in a new ez_dex_swaps with only swap transactions of the correct quote_mint
, pumpswap_dex_swaps AS (
    SELECT
        dex.*,
        quote_mint,
        event_name
    FROM {{ source('SOLANA_FLIPSIDE_DEFI', 'ez_dex_swaps') }} dex
    LEFT JOIN quote_mints_by_trade q ON dex._log_id = q.log_id
    WHERE swap_program = 'pumpswap'
    and dex.block_timestamp::date > '2025-03-19'
    {% if is_incremental() %}
        and dex.block_timestamp::date > (select dateadd('day', -3, max(block_timestamp::date)) from {{ this }})
    {% endif %}
)

-- collect anchor_cpi_events from the last three days from the latest entry in the current fact_pumpswap_trades_encoded table
, anchor_cpi_events as (
    SELECT 
        *,
        instruction:data::STRING as encoded_data,
        CASE 
            WHEN inner_index is null THEN ROW_NUMBER() OVER (PARTITION BY tx_id ORDER BY (instruction_index * 1000))
            WHEN inner_index is not null THEN ROW_NUMBER() OVER (PARTITION BY tx_id ORDER BY (instruction_index * 1000 + inner_index))
        END AS event_number,
        CONCAT(tx_id,'-',event_number, '-pumpswap') AS log_id
    FROM {{ source('SOLANA_FLIPSIDE', 'fact_events_inner') }}
    WHERE program_id = 'pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA'
    AND (substr(encoded_data, 0, 9) = '9k6unfwB8' or substr(encoded_data, 0, 9) = 'w1295DLPc')
    AND date_trunc('day', block_timestamp) > '2025-03-19'
    {% if is_incremental() %}
        and date_trunc('day', block_timestamp) > (select dateadd('day', -3, max(block_timestamp::date)) from {{ this }})
    {% endif %}
)

-- join dex swaps and anchor CPI events
, encoded_swaps as (
    SELECT 
        d.*,               
        a.encoded_data,
    FROM pumpswap_dex_swaps d
    LEFT JOIN anchor_cpi_events a
        ON d._log_id = a.log_id
    
)

select * 
from encoded_swaps
where quote_mint  in ('So11111111111111111111111111111111111111112',
                            'mSoLzYCxHdYgdzU16g5QSh3i5K3z3KZK7ytfqcJm7So',
                            'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v',
                            'Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB',
                            'DEkqHyPN7GMRJ5cArtQFAWefqbZb33Hyf6s5iCwjEonT')