
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
        SUM(usd_balance) AS treasury, 
        SUM(native_balance) AS treasury_native
    FROM {{ ref('fact_maple_treasury') }}
    GROUP BY 1
)
, net_treasury as (
    SELECT
        date,
        SUM(usd_balance) AS net_treasury,
        SUM(native_balance) AS net_treasury_native
    FROM {{ ref('fact_maple_treasury') }}
    WHERE token <> 'SYRUP'
    GROUP BY 1
)
, treasury_native as (
    SELECT
        date,
        SUM(native_balance) AS own_token_treasury_native,
        SUM(usd_balance) AS own_token_treasury
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
, supply_data as (
    SELECT * FROM {{ ref('fact_maple_supply')}}
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
    , coalesce(revenue, 0) - coalesce(total_expenses, 0) as earnings
    , coalesce(treasury.treasury, 0) as treasury_value
    , coalesce(treasury_native.own_token_treasury_native, 0) as treasury_value_native
    , coalesce(net_treasury.net_treasury, 0) as net_treasury_value
    , coalesce(tvl.tvl, 0) as net_deposits
    , coalesce(tvl.outstanding_supply, 0) as outstanding_supply
    , coalesce(tokenholders.token_holder_count, 0) as token_holder_count

    -- Token Metrics
    , coalesce(price.price, 0) as price
    , coalesce(price.market_cap, 0) as market_cap
    , coalesce(price.fdmc, 0) as fdmc
    , coalesce(price.token_volume, 0) as token_volume

    --Lending Metrics
    , coalesce(tvl.outstanding_supply, 0) as lending_loans
    , coalesce(tvl.tvl, 0) as lending_deposits

    --Cashflow Metrics
    , coalesce(fees.platform_fees, 0) + coalesce(fees.delegate_fees, 0) + coalesce(fees.fees, 0) as ecosystem_revenue
    , (coalesce(interest_fees, 0) - coalesce(platform_fees, 0) - coalesce(delegate_fees, 0)) as staking_fee_allocation
    , coalesce(fees.platform_fees, 0) as treasury_fee_allocation
    , 0.33 * coalesce(fees.delegate_fees, 0) as service_fee_allocation
    , 0.66 * coalesce(fees.delegate_fees, 0) as token_fee_allocation
    , coalesce(supply_data.buybacks_native, 0) as buybacks_native
    , coalesce(supply_data.buybacks, 0) as buybacks

    --Crypto Metrics
    , coalesce(tvl.tvl, 0) as tvl
    , coalesce(tvl.tvl, 0) - LAG(coalesce(tvl.tvl, 0)) OVER (ORDER BY date) as tvl_net_change
    --Protocol metrics
    , coalesce(treasury.treasury, 0) as treasury
    , coalesce(treasury.treasury_native, 0) as treasury_native
    , coalesce(net_treasury.net_treasury, 0) as net_treasury
    , coalesce(net_treasury.net_treasury_native, 0) as net_treasury_native
    , coalesce(treasury_native.own_token_treasury, 0) as own_token_treasury
    , coalesce(treasury_native.own_token_treasury_native, 0) as own_token_treasury_native

    --Turnover Metrics
    , coalesce(price.token_turnover_circulating, 0) as token_turnover_circulating
    , coalesce(price.token_turnover_fdv, 0) as token_turnover_fdv

    --Supply Metrics
    , coalesce(supply_data.premine_unlocks_native, 0) as premine_unlocks_native
    , coalesce(supply_data.locked_syrup_native, 0) as locked_syrup_native
    , coalesce(supply_data.emissions_native, 0) as gross_emissions_native
    , coalesce(supply_data.emissions_native, 0) * coalesce(price.price, 0) as gross_emissions
    , coalesce(supply_data.circulating_supply_native, 0) as circulating_supply_native
FROM price
LEFT JOIN fees USING(date)
LEFT JOIN revenues USING(date)
LEFT JOIN token_incentives USING(date)
LEFT JOIN tvl USING(date)
LEFT JOIN treasury USING(date)
LEFT JOIN treasury_native USING(date)
LEFT JOIN net_treasury USING(date)
LEFT JOIN tokenholders USING(date)
LEFT JOIN supply_data USING(date)