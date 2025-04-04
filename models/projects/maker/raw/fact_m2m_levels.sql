{{
    config(
        materialized="table",
        snowflake_warehouse="MAKER",
        database="maker",
        schema="raw",
        alias="fact_m2m_levels"
    )
}}

WITH treasury_tokens AS (
    SELECT token, price_address
    FROM {{ ref('dim_treasury_erc20s') }}
    
    UNION ALL
    
    SELECT 'DAI' AS token, '0x6b175474e89094c44da98b954eedeac495271d0f' AS price_address
)

SELECT 
    p.hour AS ts,
    tt.token,
    CASE WHEN tt.token = 'DAI' THEN 1 ELSE p.price END AS price
FROM ethereum_flipside.price.ez_prices_hourly p
INNER JOIN treasury_tokens tt ON p.token_address = tt.price_address
WHERE p.hour >= '2019-11-01'
  AND EXTRACT(HOUR FROM p.hour) = 23