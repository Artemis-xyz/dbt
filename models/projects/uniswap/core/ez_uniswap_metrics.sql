{{
    config(
        materialized="table",
        snowflake_warehouse="UNISWAP_SM",
        database="uniswap",
        schema="core",
        alias="ez_metrics",
    )
}}


WITH
    fees as (
        {{ dbt_utils.union_relations(
            relations=[
                ref('fact_uniswap_v3_bsc_trading_vol_fees_traders_by_pool'),
                ref('fact_uniswap_v3_base_trading_vol_fees_traders_by_pool'),
                ref('fact_uniswap_v3_blast_trading_vol_fees_traders_by_pool'),
                ref('fact_uniswap_v3_polygon_trading_vol_fees_traders_by_pool'),
                ref('fact_uniswap_v2_ethereum_trading_vol_fees_traders_by_pool'),
                ref('fact_uniswap_v3_arbitrum_trading_vol_fees_traders_by_pool'),
                ref('fact_uniswap_v3_ethereum_trading_vol_fees_traders_by_pool'),
                ref('fact_uniswap_v3_optimism_trading_vol_fees_traders_by_pool'),
                ref('fact_uniswap_v3_avalanche_trading_vol_fees_traders_by_pool')
            ]
        ) }}
    )
    , primary_supply_side_revenue_cte as (
        SELECT date, trading_fees as primary_supply_side_revenue FROM fees
    )
    , other_supply_side_revenue_cte as (
        SELECT date, 0 as other_supply_side_revenue FROM fees
    )
    , total_supply_side_revenue_cte as (
        SELECT date, trading_fees as total_supply_side_revenue FROM fees
    )
    , protocol_revenue_cte as (
        SELECT date, 0 as protocol_revenue FROM fees
    )
    , token_incentives_cte as (
        SELECT date, incentives_usd FROM {{ ref('fact_uniswap_token_incentives') }}
    )
    , operating_expenses_cte as (
        SELECT date, 0 as operating_expenses FROM {{ ref('fact_uniswap_token_incentives') }}
    )
    , total_expenses_cte as (
        SELECT date, incentives_usd as total_expenses FROM {{ ref('fact_uniswap_token_incentives') }}
    )
    , protocol_earnings_cte AS (
        SELECT date, protocol_revenue - total_expenses AS protocol_earnings FROM protocol_revenue_cte LEFT JOIN total_expenses_cte using(date)
    )
    , treasury_usd_cte AS (
        SELECT date, treasury_usd FROM {{ ref('fact_uniswap_treasury_usd') }}
    )
    , treasury_native_cte AS(
        SELECT date, treasury_native FROM {{ ref('fact_uniswap_treasury_by_token') }}
        WHERE token = 'UNI'
    )
    , net_treasury_cte AS (
        SELECT date, sum(usd_balance) as net_treasury_usd FROM {{ ref('fact_uniswap_treasury_usd') }}
        WHERE token <> 'UNI'
        GROUP BY 1
    )
    , net_deposit AS (
        SELECT date, sum(tvl) as net_deposit FROM {{ ref('ez_uniswap_metrics_by_chain') }}
        GROUP BY 1
    )
    , outstanding_supply AS (
        -- NA
    )
    , tvl AS (
        SELECT date, sum(tvl) AS tvl FROM {{ ref('ez_uniswap_metrics_by_chain') }}
        GROUP BY 1
    )
    , price_data as ({{ get_coingecko_metrics("uniswap") }})
    , tokenholders as (
        SELECT * FROM {{ ref('fact_uni_tokenholder_count') }}
    )
SELECT
    *
FROM fees