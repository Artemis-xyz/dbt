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
        {{ dbt_utils.union_relations(
            relations=[
                ref('fact_uniswap_v3_bsc_trading_vol_fees_traders_by_pool')
                , ref('fact_uniswap_v3_base_trading_vol_fees_traders_by_pool')
                ,  ref('fact_uniswap_v3_blast_trading_vol_fees_traders_by_pool')
                , ref('fact_uniswap_v3_polygon_trading_vol_fees_traders_by_pool')
                , ref('fact_uniswap_v2_ethereum_trading_vol_fees_traders_by_pool')
                , ref('fact_uniswap_v3_arbitrum_trading_vol_fees_traders_by_pool')
                , ref('fact_uniswap_v3_ethereum_trading_vol_fees_traders_by_pool')
                , ref('fact_uniswap_v3_optimism_trading_vol_fees_traders_by_pool')
                , ref('fact_uniswap_v3_avalanche_trading_vol_fees_traders_by_pool')
            ]
        ) }}
    )
    , fees_agg as(
        SELECT
            date
            , chain
            , token_fee_amount_native_symbol as token
            , SUM(token_fee_amount_native) as fees_native
        FROM fees_cte
        GROUP BY 1, 2, 3
    )
    , token_incentives_cte as (
        SELECT
            date
            , 'ethereum' as chain
            , token
            , token_incentives_native
        FROM {{ ref('fact_uniswap_token_incentives') }}
    )
    , treasury_cte AS(
        SELECT
            date
            , 'ethereum' as chain
            , token
            , treasury_native as treasury
        FROM {{ ref('fact_uniswap_treasury_by_token') }}
    )
    , treasury_native_cte AS(
        SELECT
            date
            , 'ethereum' as chain
            , token
            , treasury_native
        FROM {{ ref('fact_uniswap_treasury_by_token') }}
        WHERE token = 'UNI'
    )
    , net_treasury_cte AS(
        SELECT
            date
            , 'ethereum' as chain
            , token
            , treasury_native as net_treasury
        FROM {{ ref('fact_uniswap_treasury_by_token') }}
        WHERE token <> 'UNI'
    )
    , tvl_cte AS (
        {{ dbt_utils.union_relations(
            relations=[
                ref('fact_uniswap_bsc_tvl_by_token')
                , ref('fact_uniswap_base_tvl_by_token')
                , ref('fact_uniswap_blast_tvl_by_token')
                , ref('fact_uniswap_polygon_tvl_by_token')
                , ref('fact_uniswap_ethereum_tvl_by_token')
                , ref('fact_uniswap_arbitrum_tvl_by_token')
                , ref('fact_uniswap_optimism_tvl_by_token')
                , ref('fact_uniswap_avalanche_tvl_by_token')
            ]
        ) }}
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
    date,
    token,
    chain,
    fees_native as trading_fees,
    token_incentives_native,
    treasury,
    treasury_native,
    net_treasury,
    tvl
FROM
    fees_agg
LEFT JOIN token_incentives_cte using(date, chain, token)
LEFT JOIN treasury_native_cte using (date, chain, token)
LEFT JOIN treasury_cte using (date, chain, token)
LEFT JOIN net_treasury_cte using (date, chain, token)
LEFT JOIN tvl_agg using(date, chain,token)
order by 1 desc