{{
    config(
        materialized='table',
        snowflake_warehouse='CURVE',
    )
}}

SELECT 
    t.contract_address,
    c.name,
    c.symbol
FROM ethereum_flipside.core.ez_decoded_event_logs e
LEFT JOIN 
    ethereum_flipside.core.fact_event_logs t using(tx_hash) -- 635 entires
LEFT JOIN ethereum_flipside.core.dim_contracts c on c.address = t.contract_address
WHERE 1=1
AND e.contract_address = lower('0x6A8cbed756804B16E05E741eDaBd5cB544AE21bf') -- 634 entries
AND e.event_name = 'PlainPoolDeployed'
AND t.topic_0 = '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'