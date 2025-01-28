{{
    config(
        materialized='table',
        snowflake_warehouse='MAPLE',
    )
}}

SELECT
    block_timestamp,
    tx_hash,
    contract_address,
    p.symbol as token,
    to_address,
    raw_amount_precise / pow(10, p.decimals) as revenue_native,
    raw_amount_precise / pow(10, p.decimals) * p.price as revenue_usd
FROM
    {{ source('ETHEREUM_FLIPSIDE', 'ez_token_transfers') }} t
LEFT JOIN {{ source('ETHEREUM_FLIPSIDE_PRICE', 'ez_prices_hourly') }} p 
    ON date_trunc('hour', block_timestamp) = p.hour 
    AND p.token_address = t.contract_address
WHERE 
    (
        -- Maple Treasury
        t.to_address = lower('0xa9466eabd096449d650d5aeb0dd3da6f52fd0b19')
        -- Blue Chip Secured
        OR t.to_address = lower('0xd15b90ff80aa7e13fc69cd7ccd9fef654495e36c')
        OR (t.to_address = lower('0x6D7F31cDbE68e947fAFaCad005f6495eDA04cB12') AND t.from_address = lower('0xdC9b93A8A336fe5dc9DB97616eA2118000d70fc0'))
        OR (t.to_address = lower('0x0984af3FcB364c1f30337F9aB453f876e7Ff6D0B') 
            AND t.from_address NOT IN (lower('0xd15b90ff80aa7e13fc69cd7ccd9fef654495e36c'), lower('0x6D7F31cDbE68e947fAFaCad005f6495eDA04cB12')))
        
        -- Corporate USDC
        OR t.to_address = lower('0x687f2C038e2DAA38F8dAc0c5941d7B5E58bd8CA6')
        OR (t.to_address = lower('0x6d03aa567aE55FAd71Fd58D9A4ba44D9dc6aDc5f') AND t.from_address != lower('0x687f2C038e2DAA38F8dAc0c5941d7B5E58bd8CA6'))
        -- Corporate WETH
        OR t.to_address = lower('0xcb8770923b71b0c60c47f1b352991c7ea0b4be0f')
        OR (t.to_address = lower('0x6d03aa567aE55FAd71Fd58D9A4ba44D9dc6aDc5f') AND t.from_address != lower('0xcb8770923b71b0c60c47f1b352991c7ea0b4be0f'))
        -- High Yield Secured
        OR t.to_address = lower('0x7263d9Cd36d5cAe7B681906c0e29a4A94C0938A9')
        OR (t.to_address = lower('0x8c6a34e2b9cecee4a1fce672ba37e611b1aecebb') AND t.from_address != lower('0x7263d9cd36d5cae7b681906c0e29a4a94c0938a9'))
        -- Syrup USDC
        OR (t.to_address = lower('0xEe3cBEFF9dC14EC9710A643B7624C5BEaF20BCcb') AND t.from_address != lower('0x6c73b1ca08bbc3f44340603b1fb9e331c2abaca7'))
        -- Syrup USDT
        OR t.to_address = lower('0xE512aCb671cCE2c976B151DEC89f9aAf701Bb006')
    )
    AND contract_address <> lower('0x643c4e15d7d62ad0abec4a9bd4b001aa3ef52d66') -- exclude SYRUP transfers to treasury
