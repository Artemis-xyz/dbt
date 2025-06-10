{{
    config({
        "materialized": "table"
    })
}}


SELECT DATE_TRUNC('day', BLOCK_TIMESTAMP) AS day, SUM(AMOUNT_USD) AS total_usd
FROM ethereum_flipside.core.ez_token_transfers 
WHERE FROM_ADDRESS = lower('0x639192D54431F8c816368D3FB4107Bc168d0E871')
GROUP BY day
ORDER BY day