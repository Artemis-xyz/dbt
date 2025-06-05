{{
    config(
        materialized="table",
        snowflake_warehouse="SCALLOP",
        database="scallop",
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
        WHERE defillama_protocol_id = 1961 OR defillama_protocol_id = 5087
            -- This includes Scallop Lending and Scallop DEX Aggregator
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

    , market_data AS (
        {{ get_coingecko_metrics("scallop-2") }}
    )

SELECT
    d.date, 
    'sui' AS chain,
    'scallop' AS app,
    'DeFi' AS category,
    COALESCE(m.price, 0) AS price,
    COALESCE(m.market_cap, 0) AS market_cap,
    COALESCE(m.fdmc, 0) AS fdmc,
    COALESCE(m.token_volume, 0) AS token_volume,
    COALESCE(m.token_turnover_circulating, 0) AS token_turnover_circulating,
    COALESCE(m.token_turnover_fdv, 0) AS token_turnover_fdv,
    COALESCE(d.tvl, 0) AS tvl
FROM defillama_tvl_forwardfill d
LEFT JOIN market_data m USING (date)