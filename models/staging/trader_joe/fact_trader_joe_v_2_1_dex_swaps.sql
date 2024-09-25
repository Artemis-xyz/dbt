{{
    config(
        materialized='table',
        snowflake_warehouse='TRADER_JOE'
    )
}}


SELECT * FROM {{ref('fact_trader_joe_avalanche_v2_1_dex_swaps')}}
UNION ALL
SELECT * FROM {{ref('fact_trader_joe_arbitrum_v2_1_dex_swaps')}}