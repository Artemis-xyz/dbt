{{
    config(
        materialized='table',
        snowflake_warehouse='MAPLE',
        database='MAPLE',
        schema='core',
        alias='ez_metrics_by_pool'
    )
}}

with fees as (
    SELECT
        date
        , pool_name
        , SUM(net_interest_usd) AS fees
        , SUM(platform_fees_usd) AS platform_fees
        , SUM(delegate_fees_usd) AS delegate_fees
    FROM {{ ref('fact_maple_fees') }}
    GROUP BY 1, 2
)
, tvl as (
    SELECT
        date
        , pool_name
        --, SUM(tvl_native) AS tvl_native
        , SUM(tvl) AS tvl
        , SUM(outstanding_supply) AS outstanding_supply
    FROM {{ ref('fact_maple_agg_tvl') }}
    GROUP BY 1, 2
)
SELECT
    coalesce(fees.date, tvl.date) as date
    , coalesce(fees.pool_name, tvl.pool_name) as pool_name
    , coalesce(fees.fees, 0) as fees
    , coalesce(fees.platform_fees, 0) as platform_fees
    , coalesce(fees.delegate_fees, 0) as delegate_fees
    , coalesce(tvl.outstanding_supply, 0) as outstanding_supply

    -- Standardized Metrics

    -- Lending Metrics
    , coalesce(tvl.tvl, 0) as lending_deposits
    , coalesce(fees.fees, 0) as lending_fees
    , coalesce(tvl.outstanding_supply, 0) as lending_loans
    , coalesce(tvl.tvl, 0) + coalesce(tvl.outstanding_supply, 0) as lending_loan_capacity

    -- Crypto Metrics
    , coalesce(tvl.tvl, 0) as tvl
    , coalesce(tvl.tvl, 0) - lag(tvl.tvl, 0) over (order by coalesce(fees.date, tvl.date)) as tvl_net_change

    -- Cash Flow Metrics
    , coalesce(fees.fees, 0) + coalesce(fees.platform_fees, 0) + coalesce(fees.delegate_fees, 0) as gross_protocol_revenue
    , coalesce(fees.fees, 0) - coalesce(fees.platform_fees, 0) - coalesce(fees.delegate_fees, 0) as fee_sharing_token_cash_flow
        -- If delegate fees = 1/3 * platform fees, then this should be reflected. 
    , coalesce(fees.delegate_fees, 0) as service_cash_flow
    , 2/3 * coalesce(fees.platform_fees, 0) as treasury_cash_flow
    
FROM fees
FULL OUTER JOIN tvl ON fees.date = tvl.date AND fees.pool_name = tvl.pool_name