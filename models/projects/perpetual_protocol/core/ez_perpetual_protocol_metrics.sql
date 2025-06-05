{{
    config(
        materialized="view",
        snowflake_warehouse="PERPETUAL_PROTOCOL",
        database="perpetual_protocol",
        schema="core",
        alias="ez_metrics",
    )
}}

WITH 
    perp_data as (
        SELECT
            date
            , app
            , category
            , sum(trading_volume) as trading_volume
            , sum(unique_traders) as unique_traders
            , sum(fees) as fees
            , sum(revenue) as revenue
            , sum(tvl) as tvl
            , sum(tvl_growth) as tvl_growth
            -- standardize metrics
            , sum(perp_volume) as perp_volume
            , sum(perp_dau) as perp_dau
            , sum(ecosystem_revenue) as ecosystem_revenue
            , sum(tvl_pct_change) as tvl_pct_change
            , sum(treasury_cash_flow) as treasury_cash_flow
            , sum(fee_sharing_token_cash_flow) as fee_sharing_token_cash_flow
            , sum(treasury_cash_flow) as treasury_cash_flow
            , sum(service_cash_flow) as service_cash_flow
        FROM {{ ref("ez_perpetual_protocol_metrics_by_chain") }}
        WHERE date < to_date(sysdate())
        GROUP BY 1, 2, 3
    )
    , price as ({{ get_coingecko_metrics("perpetual-protocol") }})

    , token_incentives as (
        select
            date,
            SUM(total_token_incentives) as token_incentives
        from {{ref('fact_perpetual_token_incentives')}}
        group by 1
    )

SELECT
    date
    , app
    , category
    , trading_volume
    , unique_traders
    , fees
    , revenue
    , tvl_growth
    -- standardize metrics
    , perp_volume
    , perp_dau
    , tvl
    , tvl_pct_change
    , ecosystem_revenue
    , fee_sharing_token_cash_flow
    , service_cash_flow
    , treasury_cash_flow
    -- Market Data
    , price
    , market_cap
    , fdmc
    , token_turnover_circulating
    , token_turnover_fdv
    , token_volume
    , coalesce(token_incentives.token_incentives, 0) as token_incentives
FROM perp_data
LEFT JOIN price USING(date)
LEFT JOIN token_incentives USING(date)
WHERE date < to_date(sysdate())
