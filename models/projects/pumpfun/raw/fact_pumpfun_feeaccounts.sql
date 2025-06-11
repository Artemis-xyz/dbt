{{
    config(
        materialized='table',
        snowflake_warehouse='PUMPFUN',
        database='PUMPFUN',
        schema='raw',
        alias='fact_pumpfun_feeaccounts',
    )
 }}


WITH accounts_expanded AS (
  SELECT 
    decoded_instruction,
    f.value as account,
    f.index as account_index
  FROM 
    SOLANA_FLIPSIDE.CORE.FACT_DECODED_INSTRUCTIONS,
    LATERAL FLATTEN(input => decoded_instruction:accounts) f
  WHERE 
    program_id = '6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P'
    AND (event_type = 'sell' OR event_type = 'buy')
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