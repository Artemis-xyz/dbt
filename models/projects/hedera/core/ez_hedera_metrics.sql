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
        max_supply as max_supply_native,
        uncreated_tokens as uncreated_tokens_native,
        total_supply as total_supply_native,
        cumulative_burned_hbar as cumulative_burned_hbar_native,
        foundation_balances as foundation_balances_native,
        issued_supply as issued_supply_native,
        unvested_balances as unvested_balances_native,
        circulating_supply_native
    FROM {{ ref('fact_hedera_issued_supply_and_float') }}
)
, date_spine AS (
    select * 
    from {{ ref('dim_date_spine') }} 
    where date between '2020-01-01' and to_date(sysdate())
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
    , issued_supply_metrics.max_supply_native
    , issued_supply_metrics.total_supply_native
    , issued_supply_metrics.issued_supply_native
    , issued_supply_metrics.circulating_supply_native

    -- Token Turnover Metrics
    , coalesce(market_metrics.token_turnover_circulating, 0) as token_turnover_circulating
    , coalesce(market_metrics.token_turnover_fdv, 0) as token_turnover_fdv

FROM date_spine
left join issued_supply_metrics using (date)
left join market_metrics using (date)