{{
    config(
        materialized="table",
        snowflake_warehouse="DRIFT",
        database="drift",
        schema="core",
        alias="ez_metrics_by_chain",
    )
}}
WITH drift_data AS (
    SELECT 
        block_date AS date,
        -- Perp trading volume only
        SUM_IF(market_type = 1, total_volume) AS perp_trading_volume,
        'solana' AS chain
    FROM {{ ref("fact_drift_parsed_logs") }}
    GROUP BY
        block_date
)
select
    date,
    'drift' as app,
    'DeFi' as category,
    chain,
    trading_volume
from drift_data
where date < to_date(sysdate())