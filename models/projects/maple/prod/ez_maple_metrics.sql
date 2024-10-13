
{{ 
    config(
        materialized='table',
        snowflake_warehouse='MAPLE'
    )
}}



with fees as (
    SELECT
        date,
        SUM(net_interest) AS fees,
        SUM(net_interest) AS supply_side_fees
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
        DATE(block_timestamp) AS date,
        SUM(usd_balance) AS treasury_value
    FROM {{ ref('fact_maple_treasury') }}
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
    fees.fees,
    fees.supply_side_fees,
    revenues.revenue,
    token_incentives.token_incentives,
    treasury.treasury_value,
    tvl.tvl,
    price.price,
    price.market_cap,
    price.fdmc,
    price.token_turnover_circulating,
    price.token_turnover_fdv,
    price.token_volume,
    tokenholders.token_holders
FROM fees
LEFT JOIN revenues USING(date)
LEFT JOIN token_incentives USING(date)
LEFT JOIN tvl USING(date)
LEFT JOIN treasury USING(date)
LEFT JOIN price USING(date)
LEFT JOIN tokenholders USING(date)