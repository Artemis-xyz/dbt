{{
    config(
        materialized="table",
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
            , sum(ecosystem_revenue) as fees
            , sum(tvl_pct_change) as tvl_pct_change
            , sum(treasury_fee_allocation) as treasury_fee_allocation
            , sum(staking_fee_allocation) as staking_fee_allocation
            , sum(treasury_fee_allocation) as treasury_fee_allocation
            , sum(service_fee_allocation) as service_fee_allocation
        FROM {{ ref("ez_perpetual_protocol_metrics_by_chain") }}
        WHERE date < to_date(sysdate())
        GROUP BY 1, 2, 3
    )
    , market_data as ({{ get_coingecko_metrics("perpetual-protocol") }})

    , token_incentives as (
        select
            date,
            SUM(total_token_incentives) as token_incentives
        from {{ref('fact_perpetual_token_incentives')}}
        group by 1
    )

SELECT
    date

    -- Standardized Metrics
    , perp_data.perp_dau
    , perp_data.perp_volume
    , perp_data.tvl
    , perp_data.tvl_pct_change

    -- Fees Metrics
    , perp_data.fees as perp_fees
    , perp_data.fees
    , perp_data.staking_fee_allocation
    , perp_data.service_fee_allocation
    , perp_data.treasury_fee_allocation
    
    -- Financial Metrics
    , perp_data.revenue
    , coalesce(token_incentives.token_incentives, 0) as token_incentives
    , coalesce(perp_data.revenue, 0) - coalesce(token_incentives.token_incentives, 0) as earnings

    -- Market Data
    , market_data.price
    , market_data.market_cap
    , market_data.fdmc
    , market_data.token_turnover_circulating
    , market_data.token_turnover_fdv
    , market_data.token_volume
FROM perp_data
LEFT JOIN market_data USING(date)
LEFT JOIN token_incentives USING(date)
WHERE date < to_date(sysdate())
