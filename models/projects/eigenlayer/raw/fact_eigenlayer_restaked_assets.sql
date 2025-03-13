{{
    config(
        materialized="table",
        snowflake_warehouse="EIGENLAYER",
        database="EIGENLAYER",
        schema="raw",
        alias="fact_eigenlayer_restaked_assets",
    )
}}

WITH price_data AS (
    SELECT 
        hourly.token_address, symbol, name, decimals, price,
        trunc(hour, 'hour') AS truncated_hour
    FROM {{source("ETHEREUM_FLIPSIDE_PRICE", "ez_prices_hourly")}} hourly
    WHERE hour >= (SELECT MIN(date) FROM {{ref('fact_restaked_tokens')}})
), 

-- Process normal tokens (excluding special case bEIGEN)
normal_tokens AS (
    SELECT 
        restaked_tokens.date,
        restaked_tokens.token_address,
        t2.symbol AS token_symbol,
        t2.price,
        SUM(restaked_tokens.balance_token / pow(10, t2.decimals)) AS restaked_tokens_adjusted,
        SUM(restaked_tokens.balance_token / pow(10, t2.decimals) * t2.price) as amount_restaked_usd,
        'Liquid Token Restaking' AS restaking_type
    FROM 
        {{ref('fact_restaked_tokens')}} restaked_tokens
    LEFT JOIN
        {{source("ETHEREUM_FLIPSIDE_PRICE", "ez_prices_hourly")}} t2
        ON lower(restaked_tokens.token_address) = lower(t2.token_address) 
        AND t2.hour = trunc(restaked_tokens.date, 'hour')
    WHERE 
        (restaked_tokens.strategy_address != lower('0xaCB55C530Acdb2849e6d4f36992Cd8c9D50ED8F7') 
        AND restaked_tokens.token_address != lower('0x83E9115d334D248Ce39a6f36144aEaB5b3456e75'))
    GROUP BY 
        restaked_tokens.date, restaked_tokens.token_address, token_symbol, t2.price
), 

-- Process special case bEIGEN token
special_case AS (
    SELECT 
        restaked_tokens.date,
        restaked_tokens.token_address,
        'bEIGEN' AS token_symbol,
        t2.price,
        SUM(restaked_tokens.balance_token / pow(10, t2.decimals)) AS restaked_tokens_adjusted,
        SUM(restaked_tokens.balance_token / pow(10, t2.decimals) * t2.price) AS amount_restaked_usd,
        'Liquid Token Restaking' AS restaking_type 
    FROM 
        {{ref('fact_restaked_tokens')}} restaked_tokens
    LEFT JOIN price_data t2
        ON t2.symbol = 'EIGEN'
        AND t2.truncated_hour = trunc(restaked_tokens.date, 'hour')
    WHERE 
        restaked_tokens.strategy_address = lower('0xaCB55C530Acdb2849e6d4f36992Cd8c9D50ED8F7')
        AND restaked_tokens.token_address = lower('0x83E9115d334D248Ce39a6f36144aEaB5b3456e75')
    GROUP BY 
        restaked_tokens.date, restaked_tokens.token_address, t2.price
), 

-- Process restaked native ETH
restaked_eth_aggregated AS (
    SELECT
        date,
        ' ' AS token_address,
        'ETH' AS token_symbol,
        t3.price,
        SUM(restaked_native_eth) AS restaked_tokens_adjusted,
        SUM(restaked_native_eth * t3.price) AS amount_restaked_usd,
        'Native ETH' AS restaking_type
    FROM 
        {{ref('fact_restaked_native_eth')}} restaked_eth
    LEFT JOIN
        {{source("ETHEREUM_FLIPSIDE_PRICE", "ez_prices_hourly")}} t3
        ON 'ethereum' = t3.name 
        AND t3.hour = trunc(restaked_eth.date, 'hour')
    GROUP BY 
        restaked_eth.date, t3.price
), 

-- Create a date spine for continuous dates
dates AS (
    SELECT
        date
    FROM {{ ref('dim_date_spine') }}
    WHERE date BETWEEN (SELECT MIN(date) FROM restaked_eth_aggregated) 
                    AND (SELECT MAX(date) FROM restaked_eth_aggregated)
),

-- Join dates with ETH data
eth_with_dates AS (
    SELECT
        d.date,
        eth.token_address,
        eth.token_symbol,
        eth.price,
        eth.restaked_tokens_adjusted,
        eth.amount_restaked_usd,
        eth.restaking_type
    FROM dates d
    LEFT JOIN restaked_eth_aggregated eth
        ON d.date = eth.date
),

-- Forward fill missing ETH data
restaked_eth_forward_filled AS (
    SELECT
        date,
        COALESCE(token_address, ' ') AS token_address,
        'ETH' AS token_symbol,
        COALESCE(
            price,
            LAST_VALUE(price) IGNORE NULLS OVER (
                ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            )
        ) AS price,
        COALESCE(
            restaked_tokens_adjusted,
            LAST_VALUE(restaked_tokens_adjusted) IGNORE NULLS OVER (
                ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ),
            0
        ) AS restaked_tokens_adjusted,
        COALESCE(
            amount_restaked_usd,
            LAST_VALUE(amount_restaked_usd) IGNORE NULLS OVER (
                ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ),
            0
        ) AS amount_restaked_usd,
        'Native ETH' AS restaking_type
    FROM eth_with_dates
),

-- Combine all token types
all_aggregated AS (
    SELECT * FROM normal_tokens
    UNION ALL 
    SELECT * FROM special_case
    UNION ALL 
    SELECT * FROM restaked_eth_forward_filled
), 

-- Get ETH price data for conversion calculations
eth_price_data AS (
    SELECT 
        price AS eth_price_in_usd, 
        trunc(hour, 'hour') AS truncated_hour
    FROM {{source("ETHEREUM_FLIPSIDE_PRICE", "ez_prices_hourly")}}
    WHERE name = 'ethereum'
), 

-- Add ETH equivalent values
all_aggregated_with_eth AS (
    SELECT 
        a.*,
        p.eth_price_in_usd,
        -- Convert num_restaked_tokens to ETH equivalent
        CASE 
            WHEN a.token_symbol != 'ETH' 
            THEN a.restaked_tokens_adjusted * a.price / p.eth_price_in_usd 
            ELSE a.restaked_tokens_adjusted
        END AS num_restaked_eth,
        'eigenlayer' AS protocol,
        'DeFi' AS category,
        'ethereum' AS chain
    FROM all_aggregated a
    LEFT JOIN eth_price_data p 
        ON a.date = p.truncated_hour
)

-- Final output with all metrics and calculations
SELECT
    date,
    protocol,
    category,
    chain,
    token_address,
    token_symbol,
    restaking_type,
    restaked_tokens_adjusted AS num_restaked_tokens,
    amount_restaked_usd,
    num_restaked_eth,
    eth_price_in_usd,
    -- Calculate net daily change for tokens
    restaked_tokens_adjusted - LAG(restaked_tokens_adjusted) 
        OVER (PARTITION BY token_symbol ORDER BY date) AS num_restaked_tokens_net_change,
    -- Calculate net daily change for USD
    amount_restaked_usd - LAG(amount_restaked_usd) 
        OVER (PARTITION BY token_symbol ORDER BY date) AS amount_restaked_usd_net_change,
    -- Calculate net daily change for ETH
    num_restaked_eth - LAG(num_restaked_eth) 
        OVER (PARTITION BY token_symbol ORDER BY date) AS num_restaked_eth_net_change
FROM all_aggregated_with_eth
WHERE token_symbol IS NOT NULL AND token_symbol != 'EIGEN'
ORDER BY date, token_symbol
