{{
    config(
        materialized="table",
        snowflake_warehouse="BUCKET",
        database="bucket",
        schema="core",
        alias="ez_metrics_by_chain",
    )
}}

WITH 
    defillama_tvl AS (
        SELECT
            date, 
            SUM(tvl) AS tvl
        FROM {{ ref("fact_defillama_protocol_tvls") }}
        WHERE defillama_protocol_id = 3206 OR defillama_protocol_id = 5530
            -- This includes Bucket CDP and Bucket Farm
        GROUP BY 1
    )

    , date_spine AS (
        SELECT date
        FROM {{ ref("dim_date_spine") }}
        WHERE date >= (SELECT MIN(date) FROM defillama_tvl)
          AND date < to_date(sysdate())
    )

    , defillama_tvl_forwardfill AS (
        SELECT
            d.date,
            LAST_VALUE(t.tvl IGNORE NULLS) OVER (
                ORDER BY d.date
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ) AS tvl
        FROM date_spine d
        LEFT JOIN defillama_tvl t ON d.date = t.date
    )

SELECT
    d.date, 
    'sui' AS chain,
    'bucket' AS app,
    'DeFi' AS category,
    COALESCE(d.tvl, 0) AS tvl
FROM defillama_tvl_forwardfill d