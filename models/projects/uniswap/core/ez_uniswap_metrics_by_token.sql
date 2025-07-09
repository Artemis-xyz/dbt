{{
    config(
        materialized="table",
        snowflake_warehouse="UNISWAP_SM",
        database="uniswap",
        schema="core",
        alias="ez_metrics_by_token",
    )
}}


WITH
    fees_cte as (
        SELECT * FROM {{ ref('fact_uniswap_v2_ethereum_trading_vol_fees_traders_by_pool') }}
        UNION ALL
        SELECT * FROM {{ ref('fact_uniswap_v3_arbitrum_trading_vol_fees_traders_by_pool') }}
        UNION ALL
        SELECT * FROM {{ ref('fact_uniswap_v3_avalanche_trading_vol_fees_traders_by_pool') }}
        UNION ALL
        SELECT * FROM {{ ref('fact_uniswap_v3_base_trading_vol_fees_traders_by_pool') }}
        UNION ALL
        SELECT * FROM {{ ref('fact_uniswap_v3_blast_trading_vol_fees_traders_by_pool') }}
        UNION ALL
        SELECT * FROM {{ ref('fact_uniswap_v3_bsc_trading_vol_fees_traders_by_pool') }}
        UNION ALL
        SELECT * FROM {{ ref('fact_uniswap_v3_ethereum_trading_vol_fees_traders_by_pool') }}
        UNION ALL
        SELECT * FROM {{ ref('fact_uniswap_v3_optimism_trading_vol_fees_traders_by_pool') }}
        UNION ALL
        SELECT * FROM {{ ref('fact_uniswap_v3_polygon_trading_vol_fees_traders_by_pool') }}
    )
    , fees_agg as(
        SELECT
            date
            , chain
            , token_fee_amount_native_symbol as token
            , SUM(token_fee_amount_native) as fees_native
            , SUM(trading_fees) as spot_fees
        FROM fees_cte
        GROUP BY 1, 2, 3
    )
    , token_incentives_cte as (
        SELECT
            date
            , 'ethereum' as chain
            , token
            , token_incentives_native
            , token_incentives_usd
        FROM {{ ref('fact_uniswap_token_incentives') }}
    )
    , treasury_cte AS(
        SELECT
            date
            , 'ethereum' as chain
            , token
            , treasury_native as treasury_value
        FROM {{ ref('fact_uniswap_treasury_by_token') }}
    )
    , treasury_native_cte AS(
        SELECT
            date
            , 'ethereum' as chain
            , token
            , treasury_native as treasury_native_value
        FROM {{ ref('fact_uniswap_treasury_by_token') }}
        WHERE token = 'UNI'
    )
    , net_treasury_cte AS(
        SELECT
            date
            , 'ethereum' as chain
            , token
            , sum(treasury_native) as net_treasury_value
        FROM {{ ref('fact_uniswap_treasury_by_token') }}
        WHERE token <> 'UNI'
        GROUP BY 1, 2, 3
    )
    , tvl_cte AS (
        SELECT * FROM {{ ref('fact_uniswap_bsc_tvl_by_token') }}
        UNION ALL
        SELECT * FROM {{ ref('fact_uniswap_base_tvl_by_token') }}
        UNION ALL
        SELECT * FROM {{ ref('fact_uniswap_blast_tvl_by_token') }}
        UNION ALL
        SELECT * FROM {{ ref('fact_uniswap_polygon_tvl_by_token') }}
        UNION ALL
        SELECT * FROM {{ ref('fact_uniswap_ethereum_tvl_by_token') }}
        UNION ALL
        SELECT * FROM {{ ref('fact_uniswap_arbitrum_tvl_by_token') }}
        UNION ALL
        SELECT * FROM {{ ref('fact_uniswap_optimism_tvl_by_token') }}
        UNION ALL
        SELECT * FROM {{ ref('fact_uniswap_avalanche_tvl_by_token') }}
    )
    , tvl_agg as(
        SELECT
            date,
            chain,
            token,
            SUM(tvl_native) as tvl
        FROM
            tvl_cte
        GROUP BY 1,2,3
    )
SELECT
    date
    , token
    , chain
    , fees_native as trading_fees
    , treasury_value
    , treasury_native_value
    , net_treasury_value

    -- Standardized Metrics

    -- Usage/Sector Metrics
    , tvl

    -- Money Metrics
    , spot_fees
    , spot_fees as ecosystem_revenue
    , fees_native as spot_fees_native
    , fees_native as ecosystem_revenue_native
    , token_incentives_native
    , token_incentives_usd as token_incentives

    -- Treasury Metrics
    , treasury_native_value as treasury
    , net_treasury_value as net_treasury
    , treasury_native_value as own_token_treasury_native
FROM
    fees_agg
LEFT JOIN token_incentives_cte using(date, chain, token)
LEFT JOIN treasury_native_cte using (date, chain, token)
LEFT JOIN treasury_cte using (date, chain, token)
LEFT JOIN net_treasury_cte using (date, chain, token)
LEFT JOIN tvl_agg using(date, chain,token)
order by 1 desc