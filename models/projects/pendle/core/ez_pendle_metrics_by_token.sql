{{
    config(
        materialized="table",
        snowflake_warehouse="PENDLE",
        database="pendle",
        schema="core",
        alias="ez_metrics_by_token",
    )
}}

with
    swap_fees as (
        SELECT
            date
            , lower(token) as token
            , SUM(fees) as swap_fees
            , SUM(fees_native) as swap_fees_native
            , SUM(supply_side_fees) as supply_side_fees
            , SUM(supply_side_fees_native) as supply_side_fees_native
            , SUM(revenue) as swap_revenue
            , SUM(revenue_native) as swap_revenue_native
            , SUM(volume) as swap_volume
            , SUM(volume_native) as swap_volume_native
        FROM
            {{ ref('fact_pendle_trades') }}
        GROUP BY 1, 2
    )
    , yield_fees as (
        SELECT
            date
            , lower(token) as token
            , SUM(fees_native) as yield_revenue
        FROM
            {{ ref('fact_pendle_yield_fees') }}
        GROUP BY 1, 2
    )
    , tvl as (
        SELECT
            date
            , lower(symbol) as token
            , SUM(tvl_usd) as tvl
        FROM
            {{ref('fact_pendle_tvl_by_token_and_chain')}}
        GROUP BY 1, 2
    )
    , token_incentives_cte as (
        SELECT
            date
            , lower(token) as token
            , token_incentives
        FROM
            {{ref('fact_pendle_token_incentives_by_chain')}}
    )
    , date_token_spine as (
        SELECT
            distinct
            date,
            token
        FROM dim_date_spine
        CROSS JOIN (
            SELECT distinct token
            FROM swap_fees
            UNION ALL
            SELECT distinct token
            FROM yield_fees
            UNION ALL
            SELECT distinct token
            FROM tvl
            UNION ALL
            SELECT distinct token
            FROM token_incentives_cte
        )
        WHERE date between '2022-11-28' and to_date(sysdate())
    )

SELECT
    dts.date
    , dts.token

    -- Standardized Metrics
    -- Usage/Sector Metrics
    , COALESCE(t.tvl, 0) as tvl
    , coalesce(f.swap_volume, 0) as spot_volume
    , coalesce(f.swap_volume_native, 0) as spot_volume_native
    
    -- Financial Metrics
    , f.swap_fees as spot_fees
    , COALESCE(yf.yield_revenue, 0) as yield_fees
    , coalesce(f.swap_fees, 0) + coalesce(yf.yield_revenue, 0) as fees
    , coalesce(f.swap_revenue, 0) + coalesce(yf.yield_revenue, 0) as staking_fee_allocation
    , f.supply_side_fees as lp_fee_allocation

    -- Financial Statement Metrics
    , coalesce(f.swap_revenue, 0) + coalesce(yf.yield_revenue, 0) as staking_revenue
    , 0 as revenue
    , COALESCE(ti.token_incentives, 0) as token_incentives
    , revenue - token_incentives as earnings

FROM date_token_spine dts
LEFT JOIN swap_fees f USING (date, token)
LEFT JOIN yield_fees yf USING (date, token)
LEFT JOIN token_incentives_cte ti USING (date, token)
LEFT JOIN tvl t USING (date, token)
WHERE dts.date < to_date(sysdate())