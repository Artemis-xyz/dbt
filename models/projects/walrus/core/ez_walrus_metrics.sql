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
        merge_exclude_columns=["created_on"] | reject('in', var("backfill_columns", [])) | list,
        full_refresh=false,
        tags=["ez_metrics"]
    )
}}

{% set backfill_date = var("backfill_date", None) %}

WITH market_data as ({{ get_coingecko_metrics("walrus-2") }})

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

SELECT DISTINCT
    clean_parquet.date
    , clean_parquet.active_blobs
    , clean_parquet.dau
    , clean_parquet.txns

    -- Standardized Metrics
    , (COALESCE(clean_parquet.fees, 0) / 1e9) * COALESCE(market_data.price, 0) AS fees

    -- Token Metrics
    , COALESCE(market_data.price, 0) AS price
    , COALESCE(market_data.market_cap, 0) AS market_cap
    , COALESCE(market_data.fdmc, 0) AS fdmc
    , COALESCE(market_data.token_volume, 0) AS token_volume

    -- Chain Metrics
    , (COALESCE(clean_parquet.tvl, 0) / 1e9) AS total_staked_native
    , (COALESCE(clean_parquet.tvl, 0) / 1e9) * COALESCE(market_data.price, 0) AS total_staked
    
    --Turnover Metrics
    , COALESCE(market_data.token_turnover_circulating, 0) AS token_turnover_circulating
    , COALESCE(market_data.token_turnover_fdv, 0) AS token_turnover_fdv

    -- timestamp columns
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as created_on
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as modified_on
FROM clean_parquet
LEFT JOIN market_data ON clean_parquet.date = market_data.date
WHERE true
{{ ez_metrics_incremental('clean_parquet.date', backfill_date) }}
and clean_parquet.date < to_date(sysdate())