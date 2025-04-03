{{
    config(
        materialized='table',
        snowflake_warehouse='CONVEX',
        database='CONVEX',
        schema='core',
        alias='ez_metrics_by_token'
    )
}}

with 
    fees_and_revenue as (
        select
            date,
            token,
            sum(fees) as fees,
            sum(revenue) as revenue,
            sum(primary_supply_side_fees) as primary_supply_side_fees
        from {{ ref('fact_convex_revenue') }}
        group by 1, 2
    )
    , token_incentives as (
        select
            date,
            symbol as token,
            sum(token_incentives_native) as token_incentives_native,
            sum(token_incentives) as token_incentives
        from {{ ref('fact_convex_token_incentives') }}
        group by 1, 2
    )
    , tvl as (
        select
            date,
            symbol as token,
            sum(tvl) as tvl,
            sum(tvl_native) as tvl_native
        from {{ ref('fact_convex_combined_tvl') }}
        group by 1, 2
    )
    , treasury_by_token as (
        select
            date,
            token,
            sum(usd_balance) as treasury_value,
            sum(native_balance) as treasury_value_native
        from {{ ref('fact_convex_treasury_balance') }}
        group by 1, 2
    )
    , net_treasury as (
        select
            date,
            token,
            sum(usd_balance) as net_treasury_value,
            sum(native_balance) as net_treasury_native
        from {{ ref('fact_convex_treasury_balance') }}
        where token != 'CVX'
        group by 1, 2
    )
    , treasury_native as (
        select
            date,
            token,
            sum(native_balance) as treasury_native,
            sum(usd_balance) as treasury_native_value
        from {{ ref('fact_convex_treasury_balance') }}
        where token = 'CVX'
        group by 1, 2
    )   


, date_token_spine as (
    SELECT
        distinct
        date,
        token
    from {{ ref('dim_date_spine') }}
    CROSS JOIN (
                SELECT distinct token from treasury_by_token
                UNION
                SELECT distinct token from net_treasury
                UNION
                SELECT distinct token from treasury_native
                UNION
                SELECT distinct token from token_incentives
                UNION
                SELECT distinct token from tvl
                )
    where date between '2020-03-01' and to_date(sysdate())
)

select
    date_token_spine.date,
    token,
    fees_and_revenue.fees,
    fees_and_revenue.revenue,
    fees_and_revenue.primary_supply_side_fees,
    net_treasury.net_treasury_value,
    treasury_by_token.treasury_value_native,
    net_treasury.net_treasury_native,
    treasury_native.treasury_native_value,
    token_incentives.token_incentives,
    token_incentives.token_incentives_native,

    -- Standardized Metrics

    -- Lending Metrics
    , tvl.tvl as lending_deposits

    -- Crypto Metrics
    , tvl.tvl
    , tvl.tvl - lag(tvl.tvl) over (order by date) as tvl_net_change
    , tvl.tvl_native
    , tvl.tvl_native - lag(tvl.tvl_native) over (order by date) as tvl_native_net_change

    -- Cash Flow Metrics
    , (fees_and_revenue.revenue + fees_and_revenue.primary_supply_side_fees) as gross_protocol_revenue
    , fees_and_revenue.primary_supply_side_fees + (0.005 * (fees_and_revenue.revenue + fees_and_revenue.primary_supply_side_fees)) as service_cash_flow
    , 0.02*(fees_and_revenue.revenue + fees_and_revenue.primary_supply_side_fees) as treasury_cash_flow
    , 0.10*(fees_and_revenue.revenue + fees_and_revenue.primary_supply_side_fees) as fee_sharing_token_cash_flow
    , 0.045*(fees_and_revenue.revenue + fees_and_revenue.primary_supply_side_fees) as token_cash_flow

    -- Protocol Metrics
    , treasury_value.treasury_value as treasury
    , treasury_native.treasury_native as treasury_native
    , treasury_native.treasury_native - lag(treasury_native.treasury_native) over (order by date) as treasury_native_change
from date_token_spine
full outer join treasury_by_token using (date, token)
full outer join net_treasury using (date, token)
full outer join treasury_native using (date, token)
full outer join fees_and_revenue using (date, token)
full outer join token_incentives using (date, token)
full outer join tvl using (date, token)