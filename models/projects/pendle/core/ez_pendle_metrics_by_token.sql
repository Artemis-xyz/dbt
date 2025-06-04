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
            , token
            , SUM(fees_native) as swap_fees
            , SUM(supply_side_fees_native) as supply_side_fees
            , SUM(revenue_native) as swap_revenue
            , SUM(volume_usd) as swap_volume
            , SUM(volume_native) as swap_volume_native
        FROM
            {{ ref('fact_pendle_swap_fees') }}
        GROUP BY 1, 2
    )
    , yield_fees as (
        SELECT
            date
            , token
            , SUM(yield_fees_native) as yield_revenue
        FROM
            {{ ref('fact_pendle_yield_fees') }}
        GROUP BY 1, 2
    )
    , tvl as (
        SELECT
            date
            , symbol as token
            , sum(tvl_native) as tvl
        FROM
            {{ref('fact_pendle_tvl_by_token_and_chain')}}
        GROUP BY 1, 2
    )
    , token_incentives_cte as (
        SELECT
            date
            , token
            , token_incentives
        FROM
            {{ref('fact_pendle_token_incentives_by_chain')}}
    )


SELECT
    f.date
    , f.token
    , COALESCE(f.swap_fees, 0) as swap_fees
    , COALESCE(yf.yield_revenue, 0) as yield_fees
    , COALESCE(swap_fees,0) + COALESCE(yield_fees,0) as fees
    , COALESCE(f.supply_side_fees, 0) as primary_supply_side_revenue
    , 0 as secondary_supply_side_revenue
    , COALESCE(f.supply_side_fees, 0) as total_supply_side_revenue
    , COALESCE(f.swap_revenue, 0) as swap_revenue_vependle
    , COALESCE(yf.yield_revenue, 0) as yield_revenue_vependle
    , swap_revenue_vependle + yield_revenue_vependle as total_revenue_vependle
    , 0 as protocol_revenue
    , 0 as operating_expenses
    , COALESCE(ti.token_incentives, 0) as total_expenses
    , protocol_revenue - total_expenses as protocol_earnings

    -- Standardized Metrics
    
    -- Usage/Sector Metrics
    , COALESCE(t.tvl, 0) as tvl
    , coalesce(f.swap_volume, 0) as spot_volume
    , coalesce(f.swap_volume_native, 0) as spot_volume_native
    
    , f.swap_fees as spot_fees
    , COALESCE(yf.yield_revenue, 0) as yield_generated
    , coalesce(f.swap_fees, 0) + coalesce(yf.yield_revenue, 0) as ecosystem_revenue
    , coalesce(f.swap_revenue, 0) + coalesce(yf.yield_revenue, 0) as fee_sharing_token_cash_flow
    , coalesce(f.swap_revenue, 0) as spot_fee_sharing_token_cash_flow
    , coalesce(yf.yield_revenue, 0) as yield_fee_sharing_token_cash_flow
    , f.supply_side_fees as service_cash_flow
    , COALESCE(ti.token_incentives, 0) as token_incentives

FROM swap_fees f
FULL JOIN yield_fees yf USING (date, token)
FULL JOIN token_incentives_cte ti USING (date, token)
FULL JOIN tvl t USING (date, token)