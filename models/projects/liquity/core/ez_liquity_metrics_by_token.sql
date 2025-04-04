{{
    config(
        materialized='table',
        snowflake_warehouse='LIQUITY',
        database='LIQUITY',
        schema='core',
        alias='ez_metrics_by_token'
    )
}}

with  fees_and_revs as (
    select
        date,
        token,
        sum(revenue_usd) as revenue_usd,
        sum(revenue_native) as revenue_native
    from {{ ref('fact_liquity_fees_and_revs') }}
    group by 1, 2
)
, outstanding_supply as (
    select
        date,
        token,
        sum(outstanding_supply) as outstanding_supply
    from {{ ref('fact_liquity_outstanding_supply') }}
    group by 1, 2
)
, treasury as (
    SELECT
        date,
        token,
        sum(native_balance) as treasury_value,
        SUM(
            CASE WHEN token = 'LQTY'
                THEN native_balance
            END
        ) AS treasury_native_value,
        SUM(
            CASE WHEN token <> 'LQTY'
                THEN native_balance
            END
        ) AS net_treasury_value
    FROM {{ ref('fact_liquity_treasury') }}
    GROUP BY 1, 2
)
, token_incentives as (
    select
        date,
        token,
        sum(token_incentives_native) as token_incentives_native
    from {{ ref('fact_liquity_token_incentives') }}
    group by 1, 2
)
, tvl as (
    select
        date,
        token,
        sum(tvl_usd) as tvl
    from {{ ref('fact_liquity_tvl') }}
    group by 1, 2
)
, date_token_spine as (
    select
        date,
        token
    from {{ ref('dim_date_spine') }}
    cross join (select distinct token from tvl
                union
                select distinct token from outstanding_supply
                union
                select distinct token from fees_and_revs)
    where date between '2021-04-05' and to_date(sysdate())
)

select
    dts.date
    , dts.token
    , fr.revenue_usd as fees
    , fr.revenue_native as fees_native
    , fr.revenue_usd as revenue
    , fr.revenue_native as revenue_native
    , ti.token_incentives_native as token_incentives_native
    , ti.token_incentives_native as expenses_native
    , treasury.treasury_value
    , treasury.treasury_native_value
    , treasury.net_treasury_value
    , os.outstanding_supply

    -- Standardized Metrics

    -- Lending Metrics
    , tvl.tvl as lending_deposits
    , fr.revenue_usd as lending_fees
    , os.outstanding_supply as lending_loans

    -- Crypto Metrics
    , tvl.tvl
    , tvl.tvl - lag(tvl.tvl) over (order by date) as tvl_net_change

    -- Cash Flow Metrics
    , fr.revenue_usd as gross_protocol_revenue
    , fr.revenue_native as gross_protocol_revenue_native
    , ti.token_incentives_native as fee_sharing_token_cash_flow_native
    , fr.revenue_native - ti.token_incentives_native as foundation_cash_flow_native

    -- Protocol Metrics
    , treasury.treasury_value as treasury
    , treasury.treasury_native_value as treasury_native
    , treasury.treasury_native_value - lag(treasury.treasury_native_value) over (order by date) as treasury_native_net_change
from date_token_spine dts
left join tvl using (date, token)
left join outstanding_supply os using (date, token)
left join fees_and_revs fr using (date, token)
left join treasury using (date, token)
left join token_incentives ti using (date, token)