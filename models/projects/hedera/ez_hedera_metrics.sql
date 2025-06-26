{{
    config(
        materialized="table",
        snowflake_warehouse="HEDERA",
        database="hedera",
        schema="core",
        alias="ez_metrics",
    )
}}

WITH issued_supply_metrics AS (
    SELECT
        date,
        max_supply,
        uncreated_tokens,
        total_supply,
        cumulative_burned_hbar,
        foundation_balances,
        issued_supply,
        unvested_balances,
        circulating_supply_native
    FROM {{ ref('fact_hedera_issued_supply_and_float') }}
)
, date_spine AS (
    select * 
    from {{ ref('dim_date_spine') }} 
    where date between (select min(date) from issued_supply_metrics) and to_date(sysdate())
)
, market_metrics AS ({{ get_coingecko_metrics("hedera-hashgraph") }}) 

SELECT
    date_spine.date

    -- Market Metrics
    , market_metrics.price
    , market_metrics.market_cap
    , market_metrics.fdmc
    , market_metrics.token_volume

    -- Cash Flow Metrics
    , 0 as revenue

    -- Issued Supply Metrics
    , issued_supply_metrics.max_supply
    , issued_supply_metrics.uncreated_tokens
    , issued_supply_metrics.total_supply
    , issued_supply_metrics.cumulative_burned_hbar
    , issued_supply_metrics.foundation_balances
    , issued_supply_metrics.issued_supply
    , issued_supply_metrics.unvested_balances
    , issued_supply_metrics.circulating_supply_native

    -- Token Turnover Metrics
    , coalesce(market_metrics.token_turnover_circulating, 0) as token_turnover_circulating
    , coalesce(market_metrics.token_turnover_fdv, 0) as token_turnover_fdv

FROM date_spine
left join issued_supply_metrics using (date)
left join market_metrics using (date)