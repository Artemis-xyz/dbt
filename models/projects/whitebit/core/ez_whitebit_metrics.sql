{{
    config(
        materialized="table",
        snowflake_warehouse="WHITEBIT",
        database="whitebit",
        schema="core",
        alias="ez_metrics",
    )
}}

WITH 
    date_spine as (
        SELECT date
        FROM {{ ref("dim_date_spine") }}
        WHERE date >= '2022-08-05' AND date < SYSDATE()
    )
    , supply_data as (
        SELECT * FROM {{ ref("fact_whitebit_supply") }}
    )
    , revenue_data as (
        SELECT * FROM {{ ref("fact_whitebit_revenue") }}
    )
    , market_data AS (
        {{ get_coingecko_metrics("whitebit") }}
    )

SELECT 
    date
    -- Standardized Metrics

    --Token Metrics
    , COALESCE(market_data.price, 0) AS price
    , COALESCE(market_data.market_cap, 0) AS market_cap
    , COALESCE(market_data.fdmc, 0) AS fdmc
    , COALESCE(market_data.token_volume, 0) AS token_volume

    --Cashflow Metrics
    , COALESCE(revenue_data.revenue, 0) AS revenue
    , COALESCE(revenue_data.revenue_native, 0) AS revenue_native
    , CASE 
        WHEN date = '2022-08-05' THEN COALESCE(revenue_data.revenue_native, 0) + 25000000
        ELSE COALESCE(revenue_data.revenue_native, 0)
    END AS burns_native

    -- Supply Metrics
    , COALESCE(supply_data.max_supply_native, 0) AS max_supply_native
    , COALESCE(supply_data.max_supply_native - supply_data.cumulative_burns_native - revenue_data.revenue_native, 0) AS total_supply_native
    , COALESCE(supply_data.max_supply_native - supply_data.cumulative_burns_native - revenue_data.revenue_native - supply_data.cumulative_foundation_owned_supply_native, 0) AS issued_supply_native
    , COALESCE(supply_data.max_supply_native - supply_data.cumulative_burns_native - revenue_data.revenue_native - supply_data.cumulative_foundation_owned_supply_native - supply_data.total_unvested_supply_native, 0) AS circulating_supply_native

    -- Turnover Metrics
    , COALESCE(market_data.token_turnover_circulating, 0) AS token_turnover_circulating
    , COALESCE(market_data.token_turnover_fdv, 0) AS token_turnover_fdv
FROM date_spine
LEFT JOIN supply_data USING (date)
LEFT JOIN revenue_data USING (date)
LEFT JOIN market_data USING (date)








-- 