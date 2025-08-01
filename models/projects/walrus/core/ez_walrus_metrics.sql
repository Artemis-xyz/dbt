{{
    config(
        materialized="incremental",
        snowflake_warehouse="WALRUS",
        database="walrus",
        schema="core",
        alias="ez_metrics",
        incremental_strategy="merge",
        unique_key="date",
        on_schema_change="append_new_columns",
        merge_update_columns=var("backfill_columns", []),
        merge_exclude_columns=["created_on"] if not var("backfill_columns", []) else none,
        full_refresh=false,
        tags=["ez_metrics"]
    )
}}

{% set backfill_date = var("backfill_date", None) %}

WITH 
    date_spine AS (
        SELECT date
        FROM {{ ref("dim_date_spine") }}
        WHERE date >= '2025-03-25'
        AND date < to_date(sysdate())
    )

    -- Have to filter out parquet rows where tvl_net_change is not null because it caused duplicates
    , clean_parquet AS (
        SELECT
            parquet_raw:date::date AS date
            , MODE(parquet_raw:active_blobs::int) AS active_blobs
            , MODE(parquet_raw:dau::int) AS dau
            , MODE(parquet_raw:txns::int) AS txns
            , MODE(parquet_raw:fees::float) AS fees
            , MODE(parquet_raw:tvl::float) AS tvl
        FROM {{source('PROD_LANDING', 'raw_sui_ez_walrus_metrics_parquet')}}
        WHERE parquet_raw:tvl_net_change::float IS NULL
        GROUP BY 1
    )

    , market_data AS ({{ get_coingecko_metrics("walrus-2") }})

SELECT DISTINCT
    date_spine.date
    , 'walrus' AS artemis_id

    -- Standardized Metrics

    -- Market Data
    , market_data.price
    , market_data.market_cap
    , market_data.fdmc
    , market_data.token_volume

    -- Usage Data
    , clean_parquet.dau AS da_dau
    , clean_parquet.dau
    , clean_parquet.txns AS da_txns
    , clean_parquet.txns
    , clean_parquet.tvl AS total_staked_native
    , clean_parquet.tvl * COALESCE(market_data.price, 0) AS total_staked

    -- Fee Data
    , clean_parquet.fees AS fees_native
    , clean_parquet.fees * COALESCE(market_data.price, 0) AS fees
    
    --Turnover Metrics
    , market_data.token_turnover_circulating
    , market_data.token_turnover_fdv

    -- Bespoke Metrics
    , clean_parquet.active_blobs

    -- timestamp columns
    , sysdate() as created_on
    , sysdate() as modified_on
FROM date_spine
LEFT JOIN clean_parquet USING (date)
LEFT JOIN market_data USING (date)
WHERE true
{{ ez_metrics_incremental("date_spine.date", backfill_date) }}
AND date_spine.date < to_date(sysdate())