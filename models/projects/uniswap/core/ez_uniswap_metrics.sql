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
                ref('fact_uniswap_v3_bsc_trading_vol_fees_traders_by_pool')
                , ref('fact_uniswap_v3_base_trading_vol_fees_traders_by_pool')
                , ref('fact_uniswap_v3_blast_trading_vol_fees_traders_by_pool')
                , ref('fact_uniswap_v3_polygon_trading_vol_fees_traders_by_pool')
                , ref('fact_uniswap_v2_ethereum_trading_vol_fees_traders_by_pool')
                , ref('fact_uniswap_v3_arbitrum_trading_vol_fees_traders_by_pool')
                , ref('fact_uniswap_v3_ethereum_trading_vol_fees_traders_by_pool')
                , ref('fact_uniswap_v3_optimism_trading_vol_fees_traders_by_pool')
                , ref('fact_uniswap_v3_avalanche_trading_vol_fees_traders_by_pool')
            ]
        ) }}
    )
    , fees_agg AS (
        SELECT
            date,
            sum(trading_fees) as fees
        FROM fees
        GROUP BY 1
    )
    , token_incentives_cte as (
        SELECT
            date,
            token_incentives_usd
        FROM {{ ref('fact_uniswap_token_incentives') }}
    )
    , treasury_usd_cte AS (
        SELECT
            date,
            treasury_usd
        FROM {{ ref('fact_uniswap_treasury_usd') }}
    )
    , treasury_native_cte AS(
        SELECT
            date,
            treasury_native
        FROM {{ ref('fact_uniswap_treasury_by_token') }}
        WHERE token = 'UNI'
    )
    , net_treasury_cte AS (
        SELECT
            date,
            sum(usd_balance) as net_treasury_usd
        FROM {{ ref('fact_uniswap_treasury_by_token') }}
        WHERE token <> 'UNI'
        GROUP BY 1
    )
    , tvl_cte AS (
        SELECT
            date,
            sum(tvl) AS tvl
        FROM {{ ref('ez_uniswap_metrics_by_chain') }}
        GROUP BY 1
    )
    , token_turnover_metrics_cte as (
        select
            date
            , token_turnover_circulating
            , token_turnover_fdv
            , token_volume
        from {{ ref("fact_uniswap_fdv_and_turnover")}}
    )
    , price_data_cte as ({{ get_coingecko_metrics("uniswap") }})
    , tokenholder_cte as (
        SELECT * FROM {{ ref('fact_uni_tokenholder_count') }}
    )
SELECT
    date
    , fees as trading_fees
    , fees
    , fees as primary_supply_side_revenue
    , 0 as secondary_supply_side_revenue
    , fees as total_supply_side_revenue
    , 0 as protocol_revenue
    , token_incentives_usd as token_incentives
    , 0 as operating_expenses
    , token_incentives + operating_expenses as total_expenses
    , protocol_revenue - total_expenses as protocol_earnings
    , treasury_usd as treausry_value
    , treasury_native_value
    , net_treasury_usd as net_treasury_value
    , tvl as net_deposit
    , tvl
    , fdmc
    , market_cap
    , token_volume
    , token_turnover_fdv
    , token_turnover_circulating
    , tokenholder_count
FROM fees_agg
LEFT JOIN token_incentives_cte using(date)
LEFT JOIN treasury_usd_cte using(date)
LEFT JOIN treasury_native_cte using(date)
LEFT JOIN net_treasury_cte using(date)
LEFT JOIN tvl_cte using(date)
LEFT JOIN token_turnover_metrics_cte using(date)
LEFT JOIN price_data_cte using(date)
LEFT JOIN tokenholder_cte using(date)
WHERE date < to_date(sysdate())