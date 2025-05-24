{{ config(materialized="table") }}

-- Ethereum token transfers from MasterChef contracts
WITH ethereum_transfers AS (
    SELECT
        date(block_timestamp) as date,
        'ethereum' as chain,
        'SUSHI' as token,
        sum(amount) as incentives_amount_native,
        sum(amount_usd) as incentives_usd
    FROM ethereum_flipside.core.ez_token_transfers
    WHERE 
        contract_address = lower('0x6b3595068778dd592e39a122f4f5a5cf09c90fe2') -- SUSHI token
        AND from_address IN (
            lower('0xc2edad668740f1aa35e4d8f227fb8e17dca888cd'), -- MasterChef V1
            lower('0xef0881ec094552b2e128cf945ef17a6752b4ec5d')  -- MasterChef V2
        )
        -- Exclude transfers to treasury or other known non-incentive destinations
        AND to_address not in (lower('0x19b3eb3af5d93b77a5619b047de0eed7115a19e7'), lower('0xe94b5eec1fa96ceecbd33ef5baa8d00e4493f4f3')) 
    GROUP BY 1, 2, 3
),

-- Ethereum MasterChef V2 Harvest events
ethereum_v2_harvest AS (
    SELECT 
        date_trunc('day', block_timestamp) as date,
        'ethereum' as chain,
        'SUSHI' as token,
        sum(decoded_log:amount::NUMBER) as incentives_amount_raw,
        sum(decoded_log:amount::NUMBER / POW(10, 18) * price) as incentives_usd
    FROM ethereum_flipside.core.ez_decoded_event_logs e
    LEFT JOIN ethereum_flipside.price.ez_prices_hourly p
        ON date_trunc('hour', e.block_timestamp) = p.hour
        AND p.token_address = lower('0x6b3595068778dd592e39a122f4f5a5cf09c90fe2')  -- SUSHI on Ethereum
    WHERE 
        e.contract_address = lower('0xef0881ec094552b2e128cf945ef17a6752b4ec5d') -- MasterChef v2 on ethereum
        AND e.event_name = 'Harvest'
    GROUP BY 1, 2, 3
),

-- Polygon MiniChef Harvest events
polygon_harvest AS (
    SELECT 
        date_trunc('day', block_timestamp) as date,
        'polygon' as chain,
        'SUSHI' as token,
        sum(decoded_log:amount::NUMBER) as incentives_amount_raw,
        sum(decoded_log:amount::NUMBER / POW(10, 18) * price) as incentives_usd
    FROM polygon_flipside.core.ez_decoded_event_logs e
    LEFT JOIN polygon_flipside.price.ez_prices_hourly p
        ON date_trunc('hour', e.block_timestamp) = p.hour
        AND p.token_address = lower('0x0b3f868e0be5597d5db9feb59e1cadbb0fdda50a')  -- SUSHI on Polygon
    WHERE 
        e.contract_address = lower('0x0769fd68dfb93167989c6f7254cd00766fb2841f') -- MiniChef on Polygon
        AND e.event_name = 'Harvest'
    GROUP BY 1, 2, 3
),

-- Arbitrum MiniChef Harvest events
arbitrum_harvest AS (
    SELECT 
        date_trunc('day', block_timestamp) as date,
        'arbitrum' as chain,
        'SUSHI' as token,
        sum(decoded_log:amount::NUMBER) as incentives_amount_raw,
        sum(decoded_log:amount::NUMBER / POW(10, 18) * price) as incentives_usd
    FROM arbitrum_flipside.core.ez_decoded_event_logs e
    LEFT JOIN arbitrum_flipside.price.ez_prices_hourly p
        ON date_trunc('hour', e.block_timestamp) = p.hour
        AND p.token_address = lower('0xd4d42f0b6def4ce0383636770ef773390b8c61a')  -- SUSHI on Arbitrum
    WHERE 
        e.contract_address = lower('0xf4d73326c13a4fc5fd7a064217e12780e9b6d2c3') -- MiniChef on Arbitrum
        AND e.event_name = 'Harvest'
    GROUP BY 1, 2, 3
),

-- Combined results across all chains
combined_incentives AS (
    SELECT date, chain, token, incentives_amount_native, incentives_usd FROM ethereum_transfers
    UNION ALL
    SELECT date, chain, token, incentives_amount_raw as incentives_amount_native, incentives_usd FROM ethereum_v2_harvest
    UNION ALL
    SELECT date, chain, token, incentives_amount_raw as incentives_amount_native, incentives_usd FROM polygon_harvest
    UNION ALL
    SELECT date, chain, token, incentives_amount_raw as incentives_amount_native, incentives_usd FROM arbitrum_harvest
)

-- Final aggregation by date
SELECT 
    date,
    SUM(incentives_amount_native) as token_incentives_native,
    SUM(incentives_usd) as token_incentives_usd
FROM combined_incentives
--where date > '2024-05-12'
GROUP BY 1
ORDER BY 1 DESC