{{
    config(
        materialized="incremental",
        snowflake_warehouse="UNISWAP_SM",
        database="uniswap",
        schema="core",
        alias="ez_metrics",
        incremental_strategy="merge",
        unique_key="date",
        on_schema_change="append_new_columns",
        merge_update_columns=var("backfill_columns", []),
        merge_exclude_columns=["created_on"] | reject('in', var("backfill_columns", [])) | list,
        full_refresh=false,
        tags=["ez_metrics"]
    )
}}

{% set backfill_date = var("backfill_date", None) %}

WITH
    fees as (
        SELECT * FROM {{ ref('fact_uniswap_v3_bsc_trading_vol_fees_traders_by_pool') }}
        UNION ALL
        SELECT * FROM {{ ref('fact_uniswap_v3_base_trading_vol_fees_traders_by_pool') }}
        UNION ALL
        SELECT * FROM {{ ref('fact_uniswap_v3_blast_trading_vol_fees_traders_by_pool') }}
        UNION ALL
        SELECT * FROM {{ ref('fact_uniswap_v3_polygon_trading_vol_fees_traders_by_pool') }}
        UNION ALL
        SELECT * FROM {{ ref('fact_uniswap_v2_ethereum_trading_vol_fees_traders_by_pool') }}
        UNION ALL
        SELECT * FROM {{ ref('fact_uniswap_v3_arbitrum_trading_vol_fees_traders_by_pool') }}
        UNION ALL
        SELECT * FROM {{ ref('fact_uniswap_v3_ethereum_trading_vol_fees_traders_by_pool') }}
        UNION ALL
        SELECT * FROM {{ ref('fact_uniswap_v3_optimism_trading_vol_fees_traders_by_pool') }}
        UNION ALL
        SELECT * FROM {{ ref('fact_uniswap_v3_avalanche_trading_vol_fees_traders_by_pool') }}
    )
    , fees_agg AS (
        SELECT
            date,
            sum(trading_fees) as fees
        FROM fees
        GROUP BY 1
    )
    , dau_txns_volume as (
        SELECT
            block_timestamp::date as date
            , count(distinct sender) as spot_dau
            , count( distinct tx_hash) as spot_txns
            , sum(trading_volume) as spot_volume
        FROM {{ ref('ez_uniswap_dex_swaps') }}
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
            SUM(treasury_usd) as treasury_usd
        FROM {{ ref('fact_uniswap_treasury_usd') }}
        GROUP BY 1
    )
    , treasury_native_cte AS(
        SELECT
            date,
            sum(treasury_native) as treasury_native,
            sum(usd_balance) as own_token_treasury
        FROM {{ ref('fact_uniswap_treasury_by_token') }}
        WHERE token = 'UNI'
        GROUP BY 1
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
    , price_data_cte as ({{ get_coingecko_metrics("uniswap") }})
    , tokenholder_cte as (
        SELECT * 
        FROM {{ ref('fact_uni_tokenholder_count') }}
    )
    , supply_metrics as (
        SELECT
            date,
            max_supply,
            total_supply,
            issued_supply,
            circulating_supply
        FROM {{ ref("fact_uniswap_supply_data") }}
    )
SELECT
    date
    , dau_txns_volume.spot_dau as dau
    , dau_txns_volume.spot_txns as txns
    , fees as trading_fees
    , fees
    , fees as primary_supply_side_revenue
    , 0 as secondary_supply_side_revenue
    , fees as total_supply_side_revenue
    , 0 as operating_expenses
    , token_incentives_usd + operating_expenses as total_expenses
    , treasury_usd as treausry_value
    , treasury_native as treasury_native_value
    , net_treasury_usd as net_treasury_value
    , tvl as net_deposits

    -- Standardized Metrics
    
    -- Market Metrics
    , price_data_cte.price
    , price_data_cte.market_cap
    , price_data_cte.fdmc
    , price_data_cte.token_volume
    
    -- Usage/Sector Metrics
    , dau_txns_volume.spot_dau
    , dau_txns_volume.spot_txns
    , dau_txns_volume.spot_volume
    , tvl


    -- Money Metrics
    , fees as spot_fees
    , fees as service_fee_allocation

    --Financial Statement Metrics
    , 0 as revenue
    , token_incentives_usd as token_incentives
    , revenue - token_incentives as earnings

    -- Treasury Metrics
    , treasury_usd as treasury
    , own_token_treasury as own_token_treasury
    , net_treasury_usd as net_treasury

    -- Supply Metrics
    , supply_metrics.max_supply as max_supply_native
    , supply_metrics.total_supply as total_supply_native
    , supply_metrics.issued_supply as issued_supply_native
    , supply_metrics.circulating_supply as circulating_supply_native

    -- Other Metrics
    , price_data_cte.token_turnover_fdv
    , price_data_cte.token_turnover_circulating
    , tokenholder_cte.token_holder_count

    -- timestamp columns
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as created_on
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as modified_on

FROM fees_agg
LEFT JOIN dau_txns_volume using(date)
LEFT JOIN token_incentives_cte using(date)
LEFT JOIN treasury_usd_cte using(date)
LEFT JOIN treasury_native_cte using(date)
LEFT JOIN net_treasury_cte using(date)
LEFT JOIN tvl_cte using(date)
LEFT JOIN price_data_cte using(date)
LEFT JOIN tokenholder_cte using(date)
LEFT JOIN supply_metrics using(date)
where true
{{ ez_metrics_incremental('date', backfill_date) }}
and date < to_date(sysdate())