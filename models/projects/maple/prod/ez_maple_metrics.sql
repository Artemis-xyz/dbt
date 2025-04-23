
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
        date,
        SUM(net_interest_usd) AS fees,
        SUM(net_interest_usd) AS supply_side_fees,
        SUM(platform_fees_usd) AS platform_fees,
        SUM(delegate_fees_usd) AS delegate_fees
    FROM {{ ref('fact_maple_fees') }}
    GROUP BY 1
)
, revenues as (
    SELECT
        date,
        SUM(revenue) AS revenue
    FROM {{ ref('fact_maple_revenue') }}
    GROUP BY 1
)
, token_incentives as (
    SELECT
        DATE(block_timestamp) AS date,
        SUM(incentive_usd) AS token_incentives
    FROM {{ ref('fact_maple_token_incentives') }}
    GROUP BY 1
)
, tvl as (
    SELECT
        date,
        SUM(tvl) AS tvl,
        SUM(outstanding_supply) AS outstanding_supply
    FROM {{ ref('fact_maple_agg_tvl') }}
    GROUP BY 1
)
, treasury as (
    SELECT
        date,
        SUM(usd_balance) AS treasury_value
    FROM {{ ref('fact_maple_treasury') }}
    GROUP BY 1
)
, net_treasury as (
    SELECT
        date,
        SUM(usd_balance) AS net_treasury_value
    FROM {{ ref('fact_maple_treasury') }}
    WHERE token <> 'SYRUP'
    GROUP BY 1
)
, treasury_native as (
    SELECT
        date,
        SUM(native_balance) AS treasury_value_native
    FROM {{ ref('fact_maple_treasury') }}
    WHERE token = 'SYRUP'
    GROUP BY 1
)
, price as(
    {{ get_coingecko_metrics('syrup')}}
)
, tokenholders as (
    SELECT * FROM {{ ref('fact_maple_tokenholder_count')}}
)

SELECT 
    price.date,
    coalesce(fees.fees, 0) as interest_fees,
    coalesce(fees.platform_fees, 0) as platform_fees,
    coalesce(fees.delegate_fees, 0) as delegate_fees,
    coalesce(fees.fees, 0) as fees,
    coalesce(interest_fees, 0) - coalesce(platform_fees, 0) - coalesce(delegate_fees, 0) as primary_supply_side_revenue,
    coalesce(primary_supply_side_revenue, 0) as total_supply_side_revenue,
    coalesce(revenues.revenue, 0) as revenue,
    coalesce(token_incentives.token_incentives, 0) as token_incentives,
    coalesce(token_incentives.token_incentives, 0) as total_expenses,
    coalesce(revenue, 0) - coalesce(total_expenses, 0) as protocol_earnings,
    coalesce(treasury.treasury_value, 0) as treasury_value,
    coalesce(treasury_native.treasury_value_native, 0) as treasury_value_native,
    coalesce(net_treasury.net_treasury_value, 0) as net_treasury_value,
    coalesce(tvl.tvl, 0) as tvl,
    coalesce(tvl.tvl, 0) as net_deposits,
    coalesce(tvl.outstanding_supply, 0) as outstanding_supply,
    coalesce(price.price, 0) as price,
    coalesce(price.market_cap, 0) as market_cap,
    coalesce(price.fdmc, 0) as fdmc,
    price.token_turnover_circulating,
    price.token_turnover_fdv,
    price.token_volume,
    tokenholders.token_holder_count
FROM price
LEFT JOIN fees USING(date)
LEFT JOIN revenues USING(date)
LEFT JOIN token_incentives USING(date)
LEFT JOIN tvl USING(date)
LEFT JOIN treasury USING(date)
LEFT JOIN treasury_native USING(date)
LEFT JOIN net_treasury USING(date)
LEFT JOIN tokenholders USING(date)