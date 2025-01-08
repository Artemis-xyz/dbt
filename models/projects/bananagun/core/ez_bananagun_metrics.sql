{{
    config(
        materialized='table',
        snowflake_warehouse='BANANAGUN',
        database='BANANAGUN',
        schema='core',
        alias='ez_metrics'
    )
}}

SELECT 
    trade_date,
    SUM("trading_volume") as "trading_volume",
    SUM("dau") as "dau",
    SUM("daily_txns") as "daily_txns",
    SUM("fees_usd") as "fees_usd"
FROM {{ ref('fact_bananagun_all_metrics') }}
GROUP BY trade_date
ORDER BY trade_date DESC