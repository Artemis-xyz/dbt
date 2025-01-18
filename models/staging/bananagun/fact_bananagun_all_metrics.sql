{{
    config(
        materialized='table',
        snowflake_warehouse='BANANAGUN'
    )
}}

WITH all_metrics AS (
    SELECT 
        'ethereum' as chain,
        trade_date,
        trading_volume,
        dau,
        daily_txns,
        fees_usd
    FROM {{ ref('fact_bananagun_ethereum_metrics') }}

    UNION ALL

    SELECT 
        'blast' as chain,
        trade_date,
        trading_volume,
        dau,
        daily_txns,
        fees_usd
    FROM {{ ref('fact_bananagun_blast_metrics') }}

    UNION ALL

    SELECT 
        'base' as chain,
        trade_date,
        trading_volume,
        dau,
        daily_txns,
        fees_usd
    FROM {{ ref('fact_bananagun_base_metrics') }}

    UNION ALL

    SELECT 
        'solana' as chain,
        trade_date,
        trading_volume,
        dau,
        daily_txns,
        fees_usd
    FROM {{ ref('fact_bananagun_solana_metrics') }}
)

SELECT
    chain,
    trade_date,
    SUM(trading_volume) as trading_volume,
    SUM(dau) as dau,
    SUM(daily_txns) as daily_txns,
    SUM(fees_usd) as fees_usd
FROM all_metrics
GROUP BY chain, trade_date
ORDER BY trade_date DESC
