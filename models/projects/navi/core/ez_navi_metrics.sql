{{
    config(
        materialized="table",
        snowflake_warehouse="NAVI",
        database="navi",
        schema="core",
        alias="ez_metrics",
    )
}}

WITH 
    defillama_tvl AS (
        SELECT * 
        FROM {{ ref("fact_defillama_protocol_tvls") }}
        WHERE defillama_protocol_id = 3323
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
        {{ get_coingecko_metrics("navi") }}
    )

SELECT
    d.date, 
    'navi' AS app,
    'DeFi' AS category,
    m.price,
    m.market_cap,
    m.fdmc,
    m.token_volume,
    m.token_turnover_circulating,
    m.token_turnover_fdv,
    d.tvl
FROM defillama_tvl_forwardfill d
LEFT JOIN market_data m USING (date)
