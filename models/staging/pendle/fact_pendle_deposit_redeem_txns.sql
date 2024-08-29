{{
    config(
        materialized="table",
        snowflake_warehouse="PENDLE"
    )
}}

WITH sy_addresses AS (
    SELECT sy_address
    FROM pc_dbt_db.prod.dim_pendle_ethereum_market_metadata
)

SELECT
    DECODED_LOG:tokenIn::STRING as token_address,
    DECODED_LOG:amountDeposited as amount,
    contract_address as sy_address,
    *
FROM ethereum_flipside.core.ez_decoded_event_logs
WHERE contract_address IN (SELECT DISTINCT(sy_address) FROM sy_addresses)
AND event_name = 'Deposit'
AND  DECODED_LOG:tokenInt::STRING = lower('0xcd5fe23c85820f7b72d0926fc9b05b43e359b7ee')
UNION ALL
SELECT
    DECODED_LOG:tokenOut::STRING as token_address,
    - DECODED_LOG:amountTokenOut as amount,
    contract_address as sy_address,
    *
FROM ethereum_flipside.core.ez_decoded_event_logs
WHERE contract_address IN (SELECT DISTINCT(sy_address) FROM sy_addresses)
AND event_name = 'Redeem'
AND  DECODED_LOG:tokenOut::STRING = lower('0xcd5fe23c85820f7b72d0926fc9b05b43e359b7ee')