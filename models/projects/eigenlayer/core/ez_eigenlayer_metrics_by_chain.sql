{{
    config(
        materialized="table",
        snowflake_warehouse="EIGENLAYER",
        database="EIGENLAYER",
        schema="CORE",
        alias="ez_metrics_by_chain",
    )
}}

WITH price_data AS (
    SELECT 
        hourly.token_address, symbol, name, decimals, price,
        trunc(hour, 'hour') AS truncated_hour
    FROM {{source("ETHEREUM_FLIPSIDE_PRICE", "ez_prices_hourly")}} hourly --ethereum_flipside.price.ez_prices_hourly hourly
    WHERE hour >= (SELECT MIN(date) FROM {{ref('fact_restaked_tokens')}}) --EIGENLAYER.PROD_RAW.FACT_RESTAKED_TOKENS) 
), normal_tokens AS (
    SELECT 
        restaked_tokens.date,
        restaked_tokens.token_address,
        t2.symbol AS token_symbol,
        t2.price,
        SUM ( restaked_tokens.balance_token  / pow(10, t2.decimals) ) AS restaked_tokens_adjusted,
        SUM ( restaked_tokens.balance_token  / pow(10, t2.decimals)  * t2.price ) as amount_restaked_usd,
        'Liquid Token Restaking' AS restaking_type
    FROM 
        {{ref('fact_restaked_tokens')}} restaked_tokens --EIGENLAYER.PROD_RAW.FACT_RESTAKED_TOKENS restaked_tokens
    LEFT JOIN
        {{source("ETHEREUM_FLIPSIDE_PRICE", "ez_prices_hourly")}} t2 --ethereum_flipside.price.ez_prices_hourly t2
        on lower(restaked_tokens.token_address) = lower(t2.token_address) and t2.hour = trunc(restaked_tokens.date, 'hour')
    WHERE (restaked_tokens.strategy_address != lower('0xaCB55C530Acdb2849e6d4f36992Cd8c9D50ED8F7') AND restaked_tokens.token_address != lower('0x83E9115d334D248Ce39a6f36144aEaB5b3456e75'))
    GROUP BY restaked_tokens.date, restaked_tokens.token_address, token_symbol, t2.price
), special_case AS (
    SELECT 
        restaked_tokens.date,
        restaked_tokens.token_address,
        'bEIGEN' AS token_symbol,
        t2.price,
        SUM (restaked_tokens.balance_token / pow(10, t2.decimals)) AS restaked_tokens_adjusted,
        SUM (restaked_tokens.balance_token / pow(10, t2.decimals) * t2.price) AS amount_restaked_usd,
        'Liquid Token Restaking' AS restaking_type 
    FROM 
        {{ref('fact_restaked_tokens')}} restaked_tokens--EIGENLAYER.PROD_RAW.FACT_RESTAKED_TOKENS restaked_tokens
    LEFT JOIN price_data t2
        ON t2.symbol = 'EIGEN'
        AND t2.truncated_hour = trunc(restaked_tokens.date, 'hour')
    WHERE 
        restaked_tokens.strategy_address = lower('0xaCB55C530Acdb2849e6d4f36992Cd8c9D50ED8F7')
        AND restaked_tokens.token_address = lower('0x83E9115d334D248Ce39a6f36144aEaB5b3456e75')
    GROUP BY restaked_tokens.date, restaked_tokens.token_address, t2.price
), restaked_eth_aggregated AS (
    SELECT
        date,
        ' ' AS token_address,
        'ETH' AS token_symbol,
        t3.price,
        SUM(restaked_native_eth) AS restaked_tokens_adjusted,
        SUM(restaked_native_eth * t3.price) AS amount_restaked_usd,
        'Native ETH' AS restaking_type
    FROM 
        {{ref('fact_restaked_native_eth')}} restaked_eth--EIGENLAYER.PROD_RAW.FACT_RESTAKED_NATIVE_ETH restaked_eth
    LEFT JOIN
        {{source("ETHEREUM_FLIPSIDE_PRICE", "ez_prices_hourly")}} t3 --ETHEREUM_FLIPSIDE.PRICE.ez_prices_hourly t3
        on 'ethereum' = t3.name and t3.hour = trunc(restaked_eth.date, 'hour')
    GROUP BY restaked_eth.date, t3.price
), all_aggregated AS (
    SELECT * FROM normal_tokens
    UNION ALL 
    SELECT * FROM special_case
    UNION ALL 
    SELECT * FROM restaked_eth_aggregated
), eth_price_data AS (
    -- Fetch Ethereum price separately to use for ETH conversion
    SELECT 
        price AS eth_price_in_usd, 
        trunc(hour, 'hour') AS truncated_hour
    FROM {{source("ETHEREUM_FLIPSIDE_PRICE", "ez_prices_hourly")}} --FROM ETHEREUM_FLIPSIDE.PRICE.EZ_PRICES_HOURLY
    WHERE name = 'ethereum'
), all_aggregated_with_eth AS (
    -- Convert restaked tokens into ETH equivalent
    SELECT 
        a.*,
        p.eth_price_in_usd,
        -- Convert num_restaked_tokens to ETH equivalent
        CASE 
            WHEN a.token_symbol != 'ETH' 
            THEN a.restaked_tokens_adjusted * a.price / p.eth_price_in_usd 
            ELSE a.restaked_tokens_adjusted
        END AS num_restaked_eth 
    FROM all_aggregated a
    LEFT JOIN eth_price_data p 
        ON a.date = p.truncated_hour
), ez_output AS ( 
    SELECT
        date,
        num_restaked_eth,
        token_symbol,
        num_restaked_eth * eth_price_in_usd AS amount_restaked_usd
    FROM all_aggregated_with_eth
    WHERE token_symbol is not NULL AND token_symbol != 'EIGEN'
), sums AS (
    SELECT 
        date,
        'eigenlayer' AS protocol,
        'DeFi' AS category,
        'ethereum' AS chain,
        SUM(num_restaked_eth) AS num_restaked_eth,
        SUM(amount_restaked_usd) AS amount_restaked_usd,
        -- Calculate net daily change using LAG()
    FROM ez_output
    GROUP BY date
)
SELECT 
    *,
    num_restaked_eth - LAG(num_restaked_eth) 
        OVER (ORDER BY date)  AS num_restaked_eth_net_change,

    amount_restaked_usd - LAG(amount_restaked_usd) 
        OVER (ORDER BY date)  AS amount_restaked_usd_net_change
FROM sums