{{
    config(
        materialized='table',
        snowflake_warehouse='MAPLE',
    )
}}

SELECT
    block_timestamp,
    contract_address,
    to_address,
    raw_amount_precise / pow(10, p.decimals) as revenue_native,
    raw_amount_precise / pow(10, p.decimals) * p.price as revenue_usd
FROM
    {{ source('ETHEREUM_FLIPSIDE', 'ez_token_transfers') }} t
LEFT JOIN {{ source('ETHEREUM_FLIPSIDE', 'ez_prices_hourly') }} p 
    ON date_trunc('hour', block_timestamp) = p.hour 
    AND p.token_address = t.contract_address
WHERE t.to_address = '0xa9466eabd096449d650d5aeb0dd3da6f52fd0b19'
OR t.to_address = '0xd15b90ff80aa7e13fc69cd7ccd9fef654495e36c'
OR t.to_address = '0x687f2C038e2DAA38F8dAc0c5941d7B5E58bd8CA6'
OR t.to_address = '0x94b8dcbe4c7841B54170925b67918a6312154C9c'
OR t.to_address = '0x8c6a34E2b9CeceE4a1fce672ba37e611B1AECebB'
OR (t.to_address = '0x0984af3FcB364c1f30337F9aB453f876e7Ff6D0B' 
    AND t.from_address NOT IN ('0xd15b90ff80aa7e13fc69cd7ccd9fef654495e36c', '0x6D7F31cDbE68e947fAFaCad005f6495eDA04cB12'))
OR t.to_address = '0x7263d9Cd36d5cAe7B681906c0e29a4A94C0938A9'
OR (t.to_address = '0x6D7F31cDbE68e947fAFaCad005f6495eDA04cB12' 
    AND t.from_address = '0xdC9b93A8A336fe5dc9DB97616eA2118000d70fc0')