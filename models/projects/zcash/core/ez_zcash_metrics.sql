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
        merge_exclude_columns=["created_on"] | reject('in', var("backfill_columns", [])) | list,
        full_refresh=false,
        tags=["ez_metrics"]
    )
}}

{% set backfill_date = var("backfill_date", None) %}

with
    fundamental_data as (
        select
            date
            , txns
            , gas_usd as fees
            , gas as fees_native
        from {{ ref("fact_zcash_gas_gas_usd_txns") }}
        {{ ez_metrics_incremental('date', backfill_date) }}
    )
    , github_data as ({{ get_github_metrics("zcash") }})
    , price_data as ({{ get_coingecko_metrics('zcash') }})

select
    f.date

    -- Standardized Metrics
    -- Market Data
    , price
    , market_cap
    , fdmc
    , token_volume
    --chain metrics
    , txns as chain_txns
    , fees as ecosystem_revenue
    , fees_native as ecosystem_revenue_native
    -- Developer Metrics
    , weekly_commits_core_ecosystem
    , weekly_commits_sub_ecosystem
    , weekly_developers_core_ecosystem
    , weekly_developers_sub_ecosystem
    , token_turnover_circulating
    , token_turnover_fdv
    -- timestamp columns
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as created_on
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as modified_on
from fundamental_data f
left join github_data using (date)
left join price_data on f.date = price_data.date
{{ ez_metrics_incremental('f.date', backfill_date) }}
and f.date < to_date(sysdate())
