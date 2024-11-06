{{
    config(
        materialized = 'table',
        snowflake_warehouse = 'GMX'
    )
}}

SELECT * FROM {{ ref('fact_gmx_v2_arbitrum_trade_prices') }}
UNION ALL
SELECT * FROM {{ ref('fact_gmx_v2_avalanche_trade_prices') }}   