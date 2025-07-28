{{
    config(
        materialized='table',
        snowflake_warehouse='PUMPFUN',
        alias='fact_pump_feeaccounts',
    )
 }}


WITH accounts_expanded AS (
  SELECT 
    decoded_instruction,
    block_timestamp,
    f.value as account,
    f.index as account_index
  FROM 
    {{ source('SOLANA_FLIPSIDE', 'fact_decoded_instructions') }},
    LATERAL FLATTEN(input => decoded_instruction:accounts) f
  WHERE 
    program_id = '6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P'
    AND event_type in ('sell', 'buy')
)
SELECT 
  account:pubkey::STRING AS fee_recipient,
  COUNT(*) AS frequency
FROM 
  accounts_expanded
WHERE 
  account:name::STRING = 'feeRecipient'
GROUP BY 
  fee_recipient
ORDER BY 
  frequency DESC