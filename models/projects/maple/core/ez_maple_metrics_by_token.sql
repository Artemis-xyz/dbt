
{{ 
    config(
        materialized='table',
        snowflake_warehouse='MAPLE',
        database='MAPLE',
        schema='core',
        alias='ez_metrics_by_token'
    )
}}

with fees as (
    SELECT
        date,
        asset as token,
        SUM(net_interest_native) AS fees_native,
        SUM(platform_fees_native) AS platform_fees_native,
        SUM(delegate_fees_native) AS delegate_fees_native
    FROM {{ ref('fact_maple_fees') }}
    GROUP BY 1, 2
)
, revenues as (
    SELECT
        date,
        token,
        SUM(revenue_native) AS revenue_native
    FROM {{ ref('fact_maple_revenue') }}
    GROUP BY 1, 2
)
, token_incentives as (
    SELECT
        DATE(block_timestamp) AS date,
        token,
        SUM(incentive_native) AS token_incentives_native
    FROM {{ ref('fact_maple_token_incentives') }}
    GROUP BY 1, 2
)
-- , tvl as (
--     SELECT
--         date,
--         asset as token,
--         SUM(tvl_native) AS tvl_native
--     FROM {{ ref('fact_maple_agg_tvl') }}
--     GROUP BY 1, 2
-- )
, treasury_by_token as (
    select
        date,
        token,
        sum(usd_balance) as treasury,
        sum(native_balance) as treasury_native
    from {{ ref('fact_maple_treasury') }}
    group by 1, 2
)
, net_treasury as (
    select
        date,
        token,
        sum(usd_balance) as net_treasury,
        sum(native_balance) as net_treasury_native
    from {{ ref('fact_maple_treasury') }}
    where token != 'MPL'
    group by 1, 2
)
, treasury_native as (
    select
        date,
        token,
        sum(usd_balance) as own_token_treasury,
        sum(native_balance) as own_token_treasury_native
    from {{ ref('fact_maple_treasury') }}
    where token = 'MPL'
    group by 1, 2
)  

SELECT
    coalesce(fees.date, revenues.date, token_incentives.date, treasury_by_token.date, net_treasury.date, treasury_native.date) as date,
    coalesce(fees.token, revenues.token, token_incentives.token, treasury_by_token.token, net_treasury.token, treasury_native.token) as token,
    fees.fees_native as interest_fees_native,
    fees.platform_fees_native as platform_fees_native,
    fees.delegate_fees_native as delegate_fees_native,
    fees.fees_native - fees.platform_fees_native - fees.delegate_fees_native as supply_side_revenue_native,
    supply_side_revenue_native as total_supply_side_revenue_native,
    revenues.revenue_native,
    token_incentives.token_incentives_native,
    token_incentives.token_incentives_native as expenses_native,
    revenues.revenue_native - token_incentives.token_incentives_native as protocol_earnings_native
    -- , tvl.tvl_native
    -- , tvl.tvl_native as net_deposits_native
    , treasury_native.own_token_treasury as treasury_value_native
    , net_treasury.net_treasury as net_treasury_value

    -- Standardized Metrics

    -- Lending Metrics
    , coalesce(fees.fees_native, 0) as lending_fees_native

    -- Cash Flow Metrics
    , coalesce(revenues.revenue_native, 0) as gross_protocol_revenue_native
    , coalesce(interest_fees_native, 0) - coalesce(platform_fees_native, 0) as fee_sharing_token_cash_flow_native 
        -- If delegate fees = 1/3 * platform fees, then this should be reflected.
    , coalesce(delegate_fees_native, 0) as service_cash_flow_native
    , 2/3 * coalesce(platform_fees_native, 0) as treasury_cash_flow_native
    , coalesce(token_incentives.token_incentives_native, 0) as token_cash_flow_native

    -- Protocol Metrics
    , coalesce(treasury_by_token.treasury, 0) as treasury
    , coalesce(treasury_by_token.treasury_native, 0) as treasury_native
    , coalesce(net_treasury.net_treasury, 0) as net_treasury
    , coalesce(net_treasury.net_treasury_native, 0) as net_treasury_native
    , coalesce(treasury_native.own_token_treasury, 0) as own_token_treasury
    , coalesce(treasury_native.own_token_treasury_native, 0) as own_token_treasury_native
FROM
    fees
full join revenues using(date, token)
full join token_incentives using(date, token)
-- full join tvl using(date, token)
full join treasury_by_token using(date, token)
full join net_treasury using(date, token)
full join treasury_native using(date, token)
WHERE coalesce(fees.date, revenues.date, token_incentives.date, treasury_by_token.date, net_treasury.date, treasury_native.date) < to_date(sysdate())