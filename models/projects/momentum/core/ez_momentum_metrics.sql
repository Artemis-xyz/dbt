{{
    config(
        materialized="incremental",
        snowflake_warehouse="MOMENTUM",
        database="momentum",
        schema="core",
        alias="ez_metrics",
        incremental_strategy="merge",
        unique_key="date",
        on_schema_change="append_new_columns",
        merge_update_columns=var("backfill_columns", []),
        merge_exclude_columns=["created_on"] if not var("backfill_columns", []) else none,
        full_refresh=false,
        tags=["ez_metrics"],
    )
}}

{% set backfill_date = var("backfill_date", None) %}

WITH
    date_spine AS (
        SELECT
            date
        FROM {{ ref("dim_date_spine") }}
        WHERE date < to_date(sysdate())
            AND date >= (SELECT MIN(date) FROM {{ ref("fact_raw_momentum_spot_swaps") }})
    )
    , spot_trading_volume AS (
        SELECT 
            date
            , SUM(volume_usd) AS spot_dex_volumes
        FROM {{ ref("fact_momentum_spot_volume") }}
        GROUP BY 1
    )
    , spot_dau_txns AS (
        SELECT 
            date
            , daily_dau AS dau
            , daily_txns AS txns
        FROM {{ ref("fact_momentum_spot_dau_txns") }}
        QUALIFY ROW_NUMBER() OVER (PARTITION BY date ORDER BY date DESC) = 1
    )
    , spot_fees_revenue AS (
        SELECT 
            date
            , SUM(fees) AS fees
            , SUM(service_fee_allocation) AS service_fee_allocation
            , SUM(foundation_fee_allocation) AS foundation_fee_allocation
        FROM {{ ref("fact_momentum_spot_fees_revenue") }}
        GROUP BY 1
    )
    , tvl AS (
        SELECT 
            date
            , SUM(tvl) AS tvl
        FROM {{ ref("fact_momentum_spot_tvl") }}
        GROUP BY 1
    )
select
    date_spine.date
    , 'momentum' AS artemis_id

    -- Standardized Metrics

    -- Usage Data
    , spot_dau_txns.dau AS spot_dau
    , spot_dau_txns.dau
    , spot_dau_txns.txns AS spot_txns
    , spot_dau_txns.txns
    , spot_trading_volume.spot_dex_volumes AS spot_volume
    , tvl.tvl AS spot_tvl
    , tvl.tvl

    --Fee Data
    , spot_fees_revenue.fees AS spot_fees
    , spot_fees_revenue.fees
    , spot_fees_revenue.foundation_fee_allocation
    , spot_fees_revenue.service_fee_allocation AS lp_fee_allocation

    -- Timestamp Columns
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) AS created_on
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) AS modified_on
FROM date_spine
LEFT JOIN spot_trading_volume USING(date)
LEFT JOIN spot_dau_txns USING(date)
LEFT JOIN spot_fees_revenue USING(date)
LEFT JOIN tvl USING(date)
WHERE true
{{ ez_metrics_incremental('date_spine.date', backfill_date) }}
AND date_spine.date < to_date(sysdate())
