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
    , 'uniswap' as app
    , 'DeFi' as category
    , tvl_by_chain.chain
    , trading_fees
    , trading_fees as primary_supply_side_revenue
    , trading_fees as total_supply_side_revenue
    , 0 as secondary_supply_side_revenue
    , 0 as protocol_revenue
    , 0 as operating_expenses
    , token_incentives_usd + operating_expenses as total_expenses
    , token_incentives_usd as token_incentives
    , -token_incentives_usd as earnings
    , tvl_by_chain.tvl
    , treasury_value
    , treasury_native_value
    , net_treasury_value
    , tvl_by_chain.tvl as net_deposits
    , trading_volume_by_chain.trading_volume
    , trading_volume_by_chain.unique_traders
    , trading_volume_by_chain.gas_cost_usd

    -- Standardized Metrics

    -- Usage Metrics
    , trading_volume_by_chain.unique_traders as spot_dau
    , trading_volume_by_chain.trading_volume as spot_volume
    , trading_volume_by_chain.trading_fees as spot_fees
    , trading_volume_by_chain.trading_fees as fees
    , trading_volume_by_chain.trading_fees as service_fee_allocation

    -- Treasury Metrics
    , treasury_value as treasury
    , own_token_treasury
    , net_treasury_value as net_treasury

    -- Protocol Metrics
    , trading_volume_by_chain.gas_cost_usd as gas_cost
    , trading_volume_by_chain.gas_cost_native
from tvl_by_chain
left join token_incentives_cte using(date, chain)
left join trading_volume_by_chain using(date, chain)
left join treasury_cte using(date, chain)
left join treasury_native_cte using(date, chain)
left join net_treasury_cte using(date, chain)
where tvl_by_chain.date < to_date(sysdate())
order by 1 desc