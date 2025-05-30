{{ config(materialized="incremental", snowflake_warehouse="SUSHISWAP_SM") }}

-- Ethereum token transfers from MasterChef contracts
WITH ethereum_transfers AS (
    SELECT
        date_trunc('day', block_timestamp) as date,
        'ethereum' as chain,
        'SUSHI' as token,
        sum(amount) as incentives_amount_native,
        sum(amount_usd) as incentives_usd
    FROM {{ source('ETHEREUM_FLIPSIDE', 'ez_token_transfers') }}
    WHERE 
        contract_address = lower('0x6b3595068778dd592e39a122f4f5a5cf09c90fe2') -- SUSHI token
        AND from_address IN (
            lower('0xc2EdaD668740f1aA35E4D8f227fB8E17dcA888Cd'), -- MasterChef V1
            lower('0xEF0881eC094552b2e128Cf945EF17a6752B4Ec5d')  -- MasterChef V2
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
    FROM {{ source('ETHEREUM_FLIPSIDE', 'ez_decoded_event_logs') }} e
    LEFT JOIN {{ source('ETHEREUM_FLIPSIDE_PRICE', 'ez_prices_hourly') }} p
        ON date_trunc('hour', e.block_timestamp) = p.hour
        AND p.token_address = lower('0x6B3595068778DD592e39A122f4f5a5cF09C90fE2')  -- SUSHI on Ethereum
    WHERE 
        e.contract_address = lower('0xEF0881eC094552b2e128Cf945EF17a6752B4Ec5d') -- MasterChef v2 on ethereum
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
    FROM {{ source('ARBITRUM_FLIPSIDE', 'ez_decoded_event_logs') }} e
    LEFT JOIN {{ source('ARBITRUM_FLIPSIDE_PRICE', 'ez_prices_hourly') }} p
        ON date_trunc('hour', e.block_timestamp) = p.hour
        AND p.token_address = lower('0xd4d42F0b6DEF4CE0383636770eF773390d85c61A')  -- SUSHI on Arbitrum
    WHERE 
        e.contract_address = lower('0xF4d73326C13a4Fc5FD7A064217e12780e9Bd62c3') -- MiniChef on Arbitrum
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
    FROM {{ source('POLYGON_FLIPSIDE', 'ez_decoded_event_logs') }} e
    LEFT JOIN {{ source('POLYGON_FLIPSIDE_PRICE', 'ez_prices_hourly') }} p
        ON date_trunc('hour', e.block_timestamp) = p.hour
        AND p.token_address = lower('0x0b3f868e0be5597d5db9feb59e1cadbb0fdda50a')  -- SUSHI on Polygon 
    WHERE 
        e.contract_address = lower('0x0769fd68dFb93167989C6f7254cd0D766Fb2841F') -- MiniChef on Polygon 
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

select * from combined_incentives
{% if not is_incremental() %}
    where date < '2025-05-01'
{% else %}
    where date > (select dateadd('day', -3, max(date)) from {{ this }})
{% endif %}