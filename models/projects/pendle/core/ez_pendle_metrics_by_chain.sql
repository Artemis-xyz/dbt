{{
    config(
        materialized="table",
        snowflake_warehouse="PENDLE",
        database="pendle",
        schema="core",
        alias="ez_metrics_by_chain",
    )
}}

with
    swap_fees as (
        SELECT
            date
            , chain
            , SUM(fees_usd) as swap_fees
            , SUM(supply_side_fees) as supply_side_fees
            , SUM(revenue) as swap_revenue
        FROM
            {{ ref('fact_pendle_swap_fees') }}
        GROUP BY 1, 2
    )
    , yield_fees as (
        SELECT
            date
            , chain
            , SUM(yield_fees_usd) as yield_revenue
        FROM
            {{ ref('fact_pendle_yield_fees') }}
        GROUP BY 1, 2
    )
    , daus_txns as (
        SELECT
            date
            , chain
            , sum(daus) as daus
            , sum(daily_txns) as daily_txns
        FROM
            {{ ref('fact_pendle_daus_txns') }}
        GROUP BY 1, 2
    )
    , tvl as (
        SELECT
            date
            , chain
            , sum(amount_usd) as tvl
        FROM
            {{ref('fact_pendle_tvl_by_token_and_chain')}}
        GROUP BY 1, 2
    )
    , token_incentives_cte as (
        SELECT
            date
            , chain
            , token_incentives
            , token_incentives_native
        FROM
            {{ref('fact_pendle_token_incentives_by_chain')}}
    )


SELECT
    f.date
    , f.chain
    , COALESCE(d.daus, 0) as dau
    , COALESCE(d.daily_txns, 0) as daily_txns
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
    , COALESCE(t.tvl, 0) as net_deposits
    , 0 as outstanding_supply

    -- Standardized Metrics
    -- Usage/Sector Metrics
    , COALESCE(d.daus, 0) as spot_dau
    , COALESCE(d.daily_txns, 0) as spot_txns
    , COALESCE(t.tvl, 0) as tvl

    -- Money Metrics
    , COALESCE(yf.yield_revenue, 0) as yield_generated
    , COALESCE(f.swap_fees, 0) as spot_fees
    , COALESCE(f.swap_fees, 0) + COALESCE(yf.yield_revenue, 0) as gross_protocol_revenue
    , COALESCE(f.swap_revenue, 0) + COALESCE(yf.yield_revenue, 0) as fee_sharing_token_cash_flow
    , COALESCE(f.swap_revenue, 0) as spot_fee_sharing_token_cash_flow
    , COALESCE(yf.yield_revenue, 0) as yield_fee_sharing_token_cash_flow
    , COALESCE(f.supply_side_fees, 0) as service_cash_flow
    , COALESCE(ti.token_incentives, 0) as token_incentives
    , COALESCE(ti.token_incentives, 0) as gross_emissions
    , COALESCE(ti.token_incentives_native, 0) as gross_emissions_native
    
FROM swap_fees f
LEFT JOIN yield_fees yf USING (date, chain)
LEFT JOIN daus_txns d USING (date, chain)
LEFT JOIN token_incentives_cte ti USING (date, chain)
LEFT JOIN tvl t USING (date, chain)