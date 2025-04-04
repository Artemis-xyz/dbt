
{{ 
    config(
        materialized='table',
        snowflake_warehouse='MAPLE',
        database='MAPLE',
        schema='core',
        alias='ez_metrics'
    )
}}



with fees as (
    SELECT
        date
        , SUM(net_interest_usd) AS fees
        , SUM(net_interest_usd) AS supply_side_fees
        , SUM(platform_fees_usd) AS platform_fees
        , SUM(delegate_fees_usd) AS delegate_fees
    FROM {{ ref('fact_maple_fees') }}
    GROUP BY 1
)
, revenues as (
    SELECT
        date
        , SUM(revenue) AS revenue
    FROM {{ ref('fact_maple_revenue') }}
    GROUP BY 1
)
, token_incentives as (
    SELECT
        DATE(block_timestamp) AS date
        , SUM(incentive_usd) AS token_incentives
    FROM {{ ref('fact_maple_token_incentives') }}
    GROUP BY 1
)
, tvl as (
    SELECT
        date
        , SUM(tvl) AS tvl
        , SUM(outstanding_supply) AS outstanding_supply
    FROM {{ ref('fact_maple_agg_tvl') }}
    GROUP BY 1
)
, treasury as (
    SELECT
        date
        , SUM(usd_balance) AS treasury_value
    FROM {{ ref('fact_maple_treasury') }}
    GROUP BY 1
)
, net_treasury as (
    SELECT
        date
        , SUM(usd_balance) AS net_treasury_value
    FROM {{ ref('fact_maple_treasury') }}
    WHERE token <> 'MPL'
    GROUP BY 1
)
, treasury_native as (
    SELECT
        date
        , SUM(native_balance) AS treasury_value_native
    FROM {{ ref('fact_maple_treasury') }}
    WHERE token = 'MPL'
    GROUP BY 1
)
, price as(
    {{ get_coingecko_metrics('maple')}}
)
, tokenholders as (
    SELECT * FROM {{ ref('fact_maple_tokenholder_count')}}
)

SELECT 
    price.date
    , coalesce(fees.fees, 0) as interest_fees
    , coalesce(fees.platform_fees, 0) as platform_fees
    , coalesce(fees.delegate_fees, 0) as delegate_fees
    , coalesce(fees.fees, 0) as fees
    , coalesce(interest_fees, 0) - coalesce(platform_fees, 0) - coalesce(delegate_fees, 0) as primary_supply_side_revenue
    , coalesce(primary_supply_side_revenue, 0) as total_supply_side_revenue
    , coalesce(revenues.revenue, 0) as revenue
    , coalesce(token_incentives.token_incentives, 0) as token_incentives
    , coalesce(token_incentives.token_incentives, 0) as total_expenses
    , coalesce(revenue, 0) - coalesce(total_expenses, 0) as protocol_earnings
    , coalesce(treasury.treasury_value, 0) as treasury_value
    , coalesce(treasury_native.treasury_value_native, 0) as treasury_value_native
    , coalesce(net_treasury.net_treasury_value, 0) as net_treasury_value
    , coalesce(tvl.tvl, 0) as net_deposits
    , coalesce(tvl.outstanding_supply, 0) as outstanding_supply
    , tokenholders.token_holder_count

    -- Standardized Metrics

    -- Token Metrics
    , coalesce(price.price, 0) as price
    , coalesce(price.market_cap, 0) as market_cap
    , coalesce(price.fdmc, 0) as fdmc
    , coalesce(price.token_volume, 0) as token_volume

    -- Lending Metrics
    , coalesce(tvl.tvl, 0) as lending_deposits
    , coalesce(fees.fees, 0) as lending_fees
    , coalesce(tvl.outstanding_supply, 0) as lending_loans
    , coalesce(tvl.tvl, 0) + coalesce(tvl.outstanding_supply, 0) as lending_loan_capacity

    -- Crypto Metrics
    , coalesce(tvl.tvl, 0) as tvl
    , coalesce(tvl.tvl, 0) - lag(tvl.tvl, 0) over (order by date) as tvl_net_change

    -- Cash Flow Metrics
    , coalesce(revenues.revenue, 0) as gross_protocol_revenue
    , coalesce(interest_fees, 0) - coalesce(platform_fees, 0) - coalesce(delegate_fees, 0) as fee_sharing_token_cash_flow 
        -- If delegate fees = 1/3 * platform fees, then this should be reflected. 
    , coalesce(delegate_fees, 0) as service_cash_flow
    , coalesce(token_incentives.token_incentives, 0) as token_cash_flow
    , 2/3 * coalesce(platform_fees, 0) as treasury_cash_flow


    -- Protocol Metrics
    , coalesce(treasury.treasury_value, 0) as treasury
    , coalesce(treasury_native.treasury_value_native, 0) as treasury_native
    , coalesce(treasury_native.treasury_value_native, 0)
        - lag(treasury_native.treasury_value_native, 0) over (order by date) as treasury_native_net_change

    -- Turnover Metrics
    , coalesce(price.token_turnover_circulating, 0) as token_turnover_circulating
    , coalesce(price.token_turnover_fdv, 0) as token_turnover_fdv

FROM price
LEFT JOIN fees USING(date)
LEFT JOIN revenues USING(date)
LEFT JOIN token_incentives USING(date)
LEFT JOIN tvl USING(date)
LEFT JOIN treasury USING(date)
LEFT JOIN treasury_native USING(date)
LEFT JOIN net_treasury USING(date)
LEFT JOIN tokenholders USING(date)