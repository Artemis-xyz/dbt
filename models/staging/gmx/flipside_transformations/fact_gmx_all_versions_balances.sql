{{
    config(
        materialized='table',
        snowflake_warehouse='GMX'
    )
}}

WITH gmx_v2_arbitrum_market_addresses AS (
    SELECT full_decoded_log:data[4]:value[0][0][0][1]::STRING AS market_address
    FROM arbitrum_flipside.core.ez_decoded_event_logs  
    WHERE contract_address = LOWER('0xC8ee91A54287DB53897056e12D9819156D3822Fb')
    AND event_name = 'EventLog1' 
    AND topic_1 = LOWER('0xad5d762f1fc581b3e684cf095d93d3a2c10754f60124b09bec8bf3d76473baaf')
),
gmx_v2_avalanche_market_addresses AS (
    SELECT full_decoded_log:data[4]:value[0][0][0][1]::STRING AS market_address
    FROM avalanche_flipside.core.ez_decoded_event_logs  
    WHERE contract_address = LOWER('0xDb17B211c34240B014ab6d61d4A31FA0C0e20c26')
    AND event_name = 'EventLog1' 
    AND topic_1 = LOWER('0xad5d762f1fc581b3e684cf095d93d3a2c10754f60124b09bec8bf3d76473baaf')
),
gmx_v1_arbitrum_balances AS (
    SELECT 
        block_timestamp,
        block_timestamp::date AS date,
        address AS pool_address,
        contract_address AS token_address,
        max_by(balance_token, block_timestamp::date) AS token_balance
    FROM pc_dbt_db.prod.fact_arbitrum_address_balances_by_token   
    WHERE address = LOWER('0x489ee077994B6658eAfA855C308275EAd8097C4A')
    GROUP BY 1, 2, 3, 4
),
gmx_v1_avalanche_balances AS (
    SELECT 
        block_timestamp,
        block_timestamp::date AS date,
        address AS pool_address,
        contract_address AS token_address,
        max_by(balance_token, block_timestamp::date) AS token_balance
    FROM pc_dbt_db.prod.fact_avalanche_address_balances_by_token   
    WHERE address = LOWER('0x9ab2de34a33fb459b538c43f251eb825645e8595')
    GROUP BY 1, 2, 3, 4
),
gmx_v2_arbitrum_balances AS (
    SELECT 
        block_timestamp,
        block_timestamp::date AS date,
        address AS pool_address,
        contract_address AS token_address,
        max_by(balance_token, block_timestamp::date) AS token_balance
    FROM pc_dbt_db.prod.fact_arbitrum_address_balances_by_token
    WHERE address IN (SELECT market_address FROM gmx_v2_arbitrum_market_addresses)
    GROUP BY 1, 2, 3, 4
),  
gmx_v2_avalanche_balances AS (
    SELECT 
        block_timestamp,
        block_timestamp::date AS date,
        address AS pool_address,
        contract_address AS token_address,
        max_by(balance_token, block_timestamp::date) AS token_balance
    FROM pc_dbt_db.prod.fact_avalanche_address_balances_by_token
    WHERE address IN (SELECT market_address FROM gmx_v2_avalanche_market_addresses)
    GROUP BY 1, 2, 3, 4
)
SELECT 
    block_timestamp,
    'arbitrum' as chain,
    'v1' as version,
    date,
    pool_address,
    token_address,
    token_balance
FROM gmx_v1_arbitrum_balances

UNION ALL

SELECT 
    block_timestamp,
    'avalanche' as chain,
    'v1' as version,
    date,
    pool_address,
    token_address,
    token_balance
FROM gmx_v1_avalanche_balances

UNION ALL

SELECT 
    block_timestamp,
    'arbitrum' as chain,
    'v2' as version,
    date,
    pool_address,
    token_address,
    token_balance
FROM gmx_v2_arbitrum_balances

UNION ALL

SELECT 
    block_timestamp,
    'avalanche' as chain,
    'v2' as version,
    date,
    pool_address,
    token_address,
    token_balance
FROM gmx_v2_avalanche_balances
