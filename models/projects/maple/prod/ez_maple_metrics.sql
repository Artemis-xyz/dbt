
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
        SUM(net_interest_usd) AS supply_side_fees
    FROM {{ ref('fact_maple_fees') }}
    GROUP BY 1
)
, revenues as (
    SELECT
        DATE(block_timestamp) AS date,
        SUM(revenue_usd) AS revenue
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
        SUM(tvl) AS tvl
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
    WHERE token <> 'MPL'
    GROUP BY 1
)
, treasury_native as (
    SELECT
        date,
        SUM(native_balance) AS treasury_value_native
    FROM {{ ref('fact_maple_treasury') }}
    WHERE token = 'MPL'
    GROUP BY 1
)
, outstanding_supply as (
    SELECT
        date,
        SUM(tvl) AS tvl
    FROM {{ ref('fact_maple_agg_tvl') }}
    GROUP BY 1
)
, price as(
    {{ get_coingecko_metrics('maple')}}
)
, tokenholders as (
    SELECT * FROM {{ ref('fact_maple_tokenholder_count')}}
)

SELECT 
    fees.date,
    fees.fees as interest_fees,
    fees.supply_side_fees as primary_supply_side_revenue,
    fees.supply_side_fees as total_supply_side_revenue,
    revenues.revenue as protocol_revenue,
    token_incentives.token_incentives,
    token_incentives.token_incentives as protocol_expenses,
    treasury.treasury_value,
    treasury_native.treasury_value_native,
    net_treasury.net_treasury_value,
    tvl.tvl,
    tvl.tvl as net_deposits,
    price.price,
    price.market_cap,
    price.fdmc,
    price.token_turnover_circulating,
    price.token_turnover_fdv,
    price.token_volume,
    tokenholders.token_holder_count
FROM fees
LEFT JOIN revenues USING(date)
LEFT JOIN token_incentives USING(date)
LEFT JOIN tvl USING(date)
LEFT JOIN treasury USING(date)
LEFT JOIN treasury_native USING(date)
LEFT JOIN net_treasury USING(date)
LEFT JOIN price USING(date)
LEFT JOIN tokenholders USING(date)