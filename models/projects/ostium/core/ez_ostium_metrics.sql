{{
    config(
        materialized="incremental",
        database="ostium",
        snowflake_warehouse="OSTIUM",
        schema="core",
        alias="ez_metrics",
        incremental_strategy="merge",
        unique_key="date",
        on_schema_change="append_new_columns",
        merge_update_columns=var("backfill_columns", []),
        merge_exclude_columns=["created_on"] | reject('in', var("backfill_columns", [])) | list,
        full_refresh=false,
        tags=["ez_metrics"],
    )
}}

{% set backfill_date = var("backfill_date", None) %}

select
    date
    --Standardized Metrics
    --Usage Metrics
    , coalesce(trades, 0) as perp_txns
    , coalesce(traders, 0) as perp_dau
    , coalesce(markets, 0) as perp_markets
    , coalesce(volume_usd, 0) as perp_volume
    --Cashflow Metrics
    , coalesce(greatest(total_fees, 0), 0) as perp_fees
    , coalesce(greatest(total_fees, 0), 0) as ecosystem_revenue
    -- timestamp columns
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as created_on
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as modified_on
from {{ ref("fact_ostium_metrics") }}
where true
{{ ez_metrics_incremental('date', backfill_date) }}
and date < to_date(sysdate())