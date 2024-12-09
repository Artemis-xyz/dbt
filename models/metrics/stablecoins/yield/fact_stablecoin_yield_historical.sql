{{ config( materialized="table") }}

WITH defillama_data AS (
    SELECT 
        a.date,
        UPPER(b.chain) AS chain,
        UPPER(b.symbol) AS stablecoin,
        a.apy,
        UPPER(b.project) AS project,
        'defillama' AS source,
        MAX(a.date) OVER() AS max_date
    FROM {{ ref("fact_defillama_yield_historical") }} a
    INNER JOIN {{ ref("fact_defillama_yields") }} b
        ON a.pool = b.pool
    ORDER BY tvl_usd DESC
), artemis_data AS (
    SELECT 
        date,
        'SOLANA' AS chain,
        UPPER(market) AS stablecoin,
        daily_avg_deposit_rate AS apy,
        'DRIFT' AS project,
        'artemis' AS source,
        MAX(date) OVER() AS max_date
    FROM {{ ref("fact_drift_daily_spot_data") }}
), agg AS (
SELECT
    *
FROM defillama_data
UNION ALL
SELECT
    *
FROM artemis_data
WHERE 
    stablecoin IN (
        SELECT 
            DISTINCT stablecoin
        FROM defillama_data
    )
)
SELECT
    *
FROM agg 
WHERE date <= (SELECT MIN(max_date) FROM agg)
