{{
    config(
        materialized='incremental',
        unique_key=['tx_id', 'swap_id'],
        snowflake_warehouse='JUPITER',
    )
}}

SELECT 
    e.TX_ID,
    e.BLOCK_TIMESTAMP,
    e.PROGRAM_ID,
    e.DECODED_ARGS:id::number AS swap_id,
    
    -- Token In Details
    e.DECODED_ARGS:inAmount::FLOAT AS token_in_amount_raw,
    p_in.DECIMALS AS token_in_decimals,
    (e.DECODED_ARGS:inAmount::FLOAT / POWER(10, p_in.DECIMALS)) AS token_in_amount_native,
    ((e.DECODED_ARGS:inAmount::FLOAT / POWER(10, p_in.DECIMALS)) * p_in.PRICE) AS token_in_amount_usd,
    p_in.SYMBOL AS token_in_symbol,
    e.DECODED_INSTRUCTION:accounts[3].pubkey::string AS token_in_address,  -- sourceTokenAccount
    
    -- Token Out Details
    e.DECODED_ARGS:quotedOutAmount::FLOAT AS token_out_amount_raw,
    p_out.DECIMALS AS token_out_decimals,
    (e.DECODED_ARGS:quotedOutAmount::FLOAT / POWER(10, p_out.DECIMALS)) AS token_out_amount_native,
    ((e.DECODED_ARGS:quotedOutAmount::FLOAT / POWER(10, p_out.DECIMALS)) * p_out.PRICE) AS token_out_amount_usd,
    p_out.SYMBOL AS token_out_symbol,
    e.DECODED_INSTRUCTION:accounts[6].pubkey::string AS token_out_address, -- destinationTokenAccount
    
    -- User Info
    e.DECODED_INSTRUCTION:accounts[2].pubkey::string AS user_address, -- userTransferAuthority
    
    -- Fee Information
    (e.DECODED_ARGS:platformFeeBps / 100) AS fee_percent,
    token_in_amount_native * fee_percent AS fee_native,
    token_in_amount_usd * fee_percent AS fee_usd,
    token_in_address AS fee_token_address, -- platformFeeAccount
    p_in.SYMBOL AS fee_token_symbol,

    e.succeeded

FROM solana_flipside.core.ez_events_decoded e

-- Joining token prices based on mint addresses
LEFT JOIN SOLANA_FLIPSIDE.PRICE.EZ_PRICES_HOURLY p_in
    ON p_in.TOKEN_ADDRESS = e.DECODED_INSTRUCTION:accounts[7].pubkey::string -- sourceMint
    AND p_in.HOUR = DATE_TRUNC('hour', e.BLOCK_TIMESTAMP)

LEFT JOIN SOLANA_FLIPSIDE.PRICE.EZ_PRICES_HOURLY p_out
    ON p_out.TOKEN_ADDRESS = e.DECODED_INSTRUCTION:accounts[8].pubkey::string -- destinationMint
    AND p_out.HOUR = DATE_TRUNC('hour', e.BLOCK_TIMESTAMP)
WHERE 1=1
    AND program_id = 'JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4' 
    AND event_type = 'sharedAccountsRoute'
    {% if is_incremental() %}
    AND e.BLOCK_TIMESTAMP > (SELECT MAX(BLOCK_TIMESTAMP) FROM {{ this }})
    {% endif %}
