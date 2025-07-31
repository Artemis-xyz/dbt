{{
    config(
        materialized="incremental",
        snowflake_warehouse="CETUS",
        database="cetus",
        schema="core",
        alias="ez_metrics",
        incremental_strategy="merge",
        unique_key="date",
        on_schema_change="append_new_columns",
        merge_update_columns=var("backfill_columns", []),
        merge_exclude_columns=["created_on"] if not var("backfill_columns", []) else none,
        full_refresh=var("full_refresh", false),
        tags=["ez_metrics"],
    )
}}

{% set backfill_date = var("backfill_date", None) %}

WITH
    date_spine AS(
        SELECT
            date
        FROM {{ ref("dim_date_spine") }}
        WHERE date < to_date(sysdate())
            AND date >= (SELECT MIN(date) FROM {{ ref("fact_raw_cetus_spot_swaps") }})
    )
    , spot_trading_volume AS (
        SELECT date, coalesce(SUM(volume_usd), 0) AS spot_dex_volumes
        FROM {{ ref("fact_cetus_spot_volume") }}
        GROUP BY 1
    )
    , spot_dau_txns AS (
        SELECT date, coalesce(daily_dau, 0) AS dau, coalesce(daily_txns, 0) AS txns
        FROM {{ ref("fact_cetus_spot_dau_txns") }}
        QUALIFY ROW_NUMBER() OVER (PARTITION BY date ORDER BY date DESC) = 1
    )
    , spot_fees_revenue AS (
        SELECT date, coalesce(SUM(fees), 0) AS fees, coalesce(SUM(service_fee_allocation), 0) AS service_fee_allocation, coalesce(SUM(foundation_fee_allocation), 0) AS foundation_fee_allocation
        FROM {{ ref("fact_cetus_spot_fees_revenue") }}
        GROUP BY 1
    )
    , tvl AS (
        SELECT date, coalesce(SUM(tvl), 0) AS tvl
        FROM {{ ref("fact_cetus_spot_tvl") }}
        GROUP BY 1
    )
    , market_metrics AS ({{ get_coingecko_metrics("cetus-protocol") }})
select
    date_spine.date
    , 'cetus' AS artemis_id

    -- Standardized Metrics

    -- Market Data
    , market_data.price
    , market_data.market_cap
    , market_data.fdmc
    , market_data.token_volume

    -- Usage Data
    , spot_dau_txns.dau as spot_dau
    , spot_dau_txns.dau as dau
    , spot_dau_txns.txns as spot_txns
    , spot_dau_txns.txns as txns
    , spot_trading_volume.spot_dex_volumes as spot_volume

    -- Fee Data
    , spot_fees_revenue.fees as fees
    , spot_fees_revenue.foundation_fee_allocation as foundation_fee_allocation
    , spot_fees_revenue.service_fee_allocation as service_fee_allocation

    -- Token Turnover/Other Data
    , market_metrics.token_turnover_circulating as token_turnover_circulating
    , market_metrics.token_turnover_fdv as token_turnover_fdv
    
    -- Crypto Metrics
    , tvl.tvl as tvl
    , tvl.tvl - LAG(tvl.tvl) OVER (ORDER BY date) as tvl_net_change

    -- Turnover Metrics
    , market_metrics.token_turnover_fdv
    , market_metrics.token_turnover_circulating

    -- timestamp columns
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as created_on
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as modified_on

FROM date_spine
LEFT JOIN spot_trading_volume USING(date)
LEFT JOIN spot_dau_txns USING(date)
LEFT JOIN spot_fees_revenue USING(date)
LEFT JOIN tvl USING(date)
LEFT JOIN market_metrics USING(date)
where true
{{ ez_metrics_incremental('date_spine.date', backfill_date) }}
and date_spine.date < to_date(sysdate())