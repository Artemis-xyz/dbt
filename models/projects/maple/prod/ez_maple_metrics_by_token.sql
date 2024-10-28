
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
        SUM(net_interest_native) AS fees_native
    FROM {{ ref('fact_maple_fees') }}
    GROUP BY 1, 2
)
, revenues as (
    SELECT
        date(block_timestamp) as date,
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
, tvl as (
    SELECT
        date,
        asset as token,
        SUM(tvl_native) AS tvl_native
    FROM {{ ref('fact_maple_agg_tvl') }}
    GROUP BY 1, 2
)
, treasury as (
    SELECT
        date,
        token,
        SUM(native_balance) AS treasury_value_native
    FROM {{ ref('fact_maple_treasury') }}
    GROUP BY 1, 2
)
, treasury_native as (
    SELECT
        date,
        token,
        SUM(native_balance) AS treasury_native
    FROM {{ ref('fact_maple_treasury') }}
    WHERE token = 'MPL'
    GROUP BY 1, 2
)
, net_treasury as (
    SELECT
        date,
        token,
        SUM(native_balance) AS net_treasury_value
    FROM {{ ref('fact_maple_treasury') }}
    WHERE token <> 'MPL'
    GROUP BY 1, 2
)

SELECT
    date
    , token
    , fees.fees_native as interest_fees_native
    , fees.fees_native as supply_side_revenue_native
    , fees.fees_native as total_supply_side_revenue_native
    , revenues.revenue_native
    , token_incentives.token_incentives_native
    , token_incentives.token_incentives_native as expenses_native
    , revenues.revenue_native - token_incentives.token_incentives_native as protocol_earnings_native
    , tvl.tvl_native
    , tvl.tvl_native as net_deposits_native
    , treasury.treasury_value_native
    , treasury_native.treasury_native
    , net_treasury.net_treasury_value
FROM
    fees
full join revenues using(date, token)
full join token_incentives using(date, token)
full join tvl using(date, token)
full join treasury using(date, token)
full join treasury_native using(date, token)
full join net_treasury using(date, token)

