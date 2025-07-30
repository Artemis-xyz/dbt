{{
    config(
        materialized="incremental",
        snowflake_warehouse="ZCASH",
        database="zcash",
        schema="core",
        alias="ez_metrics",
        enabled=false,
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
        WHERE date >= '2016-10-28'
        AND date < to_date(sysdate())
    )
    , fundamental_data AS (
        SELECT
            date
            , txns
            , gas_usd as fees
            , gas as fees_native
        FROM {{ ref("fact_zcash_gas_gas_usd_txns") }}
    )
    , github_data AS ({{ get_github_metrics("zcash") }})
    , market_data AS ({{ get_coingecko_metrics('zcash') }})

select
    date_spine.date
    , 'zcash' AS artemis_id

    -- Standardized Metrics

    -- Market Data
    , market_data.price
    , market_data.market_cap
    , market_data.fdmc
    , market_data.token_volume

    --Usage Data
    , fundamental_data.txns AS chain_txns
    , fundamental_data.txns 

    -- Fee Data
    , fundamental_data.fees_native 
    , fundamental_data.fees 

    -- Developer Metrics
    , github_data.weekly_commits_core_ecosystem
    , github_data.weekly_commits_sub_ecosystem
    , github_data.weekly_developers_core_ecosystem
    , github_data.weekly_developers_sub_ecosystem

    -- Turnover Data
    , market_data.token_turnover_circulating
    , market_data.token_turnover_fdv

    -- timestamp columns
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) AS created_on
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) AS modified_on
FROM date_spine
LEFT JOIN fundamental_data USING (date)
LEFT JOIN github_data USING (date)
LEFT JOIN market_data USING (date)
WHERE true
{{ ez_metrics_incremental('date_spine.date', backfill_date) }}
AND date_spine.date < to_date(sysdate())
