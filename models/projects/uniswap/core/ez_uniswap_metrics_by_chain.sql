{{
    config(
        materialized="table",
        snowflake_warehouse="UNISWAP_SM",
        database="uniswap",
        schema="core",
        alias="ez_metrics_by_chain",
    )
}}

with
    trading_volume_by_pool as (
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
    , trading_volume_by_chain as (
        select
            trading_volume_by_pool.date,
            trading_volume_by_pool.chain,
            sum(trading_volume_by_pool.trading_volume) as trading_volume,
            sum(trading_volume_by_pool.trading_fees) as trading_fees,
            sum(trading_volume_by_pool.unique_traders) as unique_traders,
            sum(trading_volume_by_pool.gas_cost_native) as gas_cost_native,
            sum(trading_volume_by_pool.gas_cost_usd) as gas_cost_usd
        from trading_volume_by_pool
        group by trading_volume_by_pool.date, trading_volume_by_pool.chain
    )
    , token_incentives_cte as (
        SELECT
            date
            , 'ethereum' as chain
            , token_incentives_usd
        FROM {{ ref('fact_uniswap_token_incentives') }}
    )
    , treasury_cte AS(
        SELECT
            date
            , 'ethereum' as chain
            , sum(usd_balance) as treasury_value
        FROM {{ ref('fact_uniswap_treasury_by_token') }}
        GROUP BY 1, 2
    )
    , treasury_native_cte AS(
        SELECT
            date,
            'ethereum' as chain,
            sum(treasury_native) as treasury_native_value,
            sum(usd_balance) as own_token_treasury
        FROM {{ ref('fact_uniswap_treasury_by_token') }}
        WHERE token = 'UNI'
        GROUP BY 1, 2

    )
    , net_treasury_cte AS(
        SELECT
            date
            , 'ethereum' as chain
            , sum(usd_balance) as net_treasury_value
        FROM {{ ref('fact_uniswap_treasury_by_token') }}
        WHERE token <> 'UNI'
        GROUP BY 1, 2
    )
    , tvl_by_pool as (
        SELECT * FROM {{ ref('fact_uniswap_v2_ethereum_tvl_by_pool') }}
        UNION ALL
        SELECT * FROM {{ ref('fact_uniswap_v3_arbitrum_tvl_by_pool') }}
        UNION ALL
        SELECT * FROM {{ ref('fact_uniswap_v3_avalanche_tvl_by_pool') }}
        UNION ALL
        SELECT * FROM {{ ref('fact_uniswap_v3_base_tvl_by_pool') }}
        UNION ALL
        SELECT * FROM {{ ref('fact_uniswap_v3_blast_tvl_by_pool') }}
        UNION ALL
        SELECT * FROM {{ ref('fact_uniswap_v3_bsc_tvl_by_pool') }}
        UNION ALL
        SELECT * FROM {{ ref('fact_uniswap_v3_ethereum_tvl_by_pool') }}
        UNION ALL
        SELECT * FROM {{ ref('fact_uniswap_v3_optimism_tvl_by_pool') }}
        UNION ALL
        SELECT * FROM {{ ref('fact_uniswap_v3_polygon_tvl_by_pool') }}
    )
    , tvl_by_chain as (
        select
            tvl_by_pool.date,
            tvl_by_pool.chain,
            sum(tvl_by_pool.tvl) as tvl
        from tvl_by_pool
        group by tvl_by_pool.date, tvl_by_pool.chain
    )
select
    tvl_by_chain.date
    , 'uniswap' as artemis_id
    , tvl_by_chain.chain

    --Usage Data
    , trading_volume_by_chain.unique_traders as spot_dau
    , trading_volume_by_chain.unique_traders as dau
    , tvl_by_chain.tvl
    , trading_volume_by_chain.trading_volume as spot_volume

    --Fee Data
    , trading_volume_by_chain.trading_fees as spot_fees
    , trading_volume_by_chain.trading_fees as fees

    --Fee Allocation
    , trading_volume_by_chain.trading_fees as lp_fee_allocation

    --Financial Statements
    , 0 as revenue_native
    , 0 as revenue
    , coalesce(token_incentives_usd, 0) as token_incentives
    , 0 as operating_expenses
    , coalesce(revenue, 0) - coalesce(token_incentives_usd, 0) as earnings

    --Treasury Data
    , treasury_value as treasury
    , net_treasury_value as net_treasury
    , own_token_treasury

from tvl_by_chain
left join token_incentives_cte using(date, chain)
left join trading_volume_by_chain using(date, chain)
left join treasury_cte using(date, chain)
left join treasury_native_cte using(date, chain)
left join net_treasury_cte using(date, chain)
where tvl_by_chain.date < to_date(sysdate())
order by 1 desc