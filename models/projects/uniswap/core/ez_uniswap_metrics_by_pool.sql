{{
    config(
        materialized="table",
        snowflake_warehouse="UNISWAP_SM",
        database="uniswap",
        schema="core",
        alias="ez_metrics_by_pool",
    )
}}

with
    trading_volume_pool as (
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
    ),
    tvl_by_pool as (
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
select
    tvl_by_pool.date
    , 'uniswap' as artemis_id
    , tvl_by_pool.pool

    --Usage Data
    , trading_volume_pool.unique_traders as spot_dau
    , trading_volume_pool.unique_traders as dau
    , tvl_by_pool.tvl as tvl
    , trading_volume_pool.trading_volume as spot_volume

    --Fee Data
    , trading_volume_pool.trading_fees as spot_fees
    , trading_volume_pool.trading_fees as fees

    --Fee Allocation
    , trading_volume_pool.trading_fees as lp_fee_allocation

from tvl_by_pool
left join trading_volume_pool using(date, chain, version, pool)
where tvl_by_pool.date < to_date(sysdate())