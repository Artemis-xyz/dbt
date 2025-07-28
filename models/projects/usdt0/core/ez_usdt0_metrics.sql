{{ config(
    materialized="incremental",
    warehouse="USDT0",
    database="USDT0",
    schema="core",
    alias="ez_metrics",
    incremental_strategy="merge",
    unique_key="date",
    on_schema_change="append_new_columns",
    merge_update_columns=var("backfill_columns", []),
    merge_exclude_columns=["created_on"] | reject('in', var("backfill_columns", [])) | list,
    full_refresh=false,
    tags=["ez_metrics"]
) }}

{% set backfill_date = var("backfill_date", None) %}

with raw_data as (
    select
        src_block_timestamp::date as date,
        count(distinct src_address) as bridge_dau,
        count(*) as bridge_txns,
        sum(amount_sent) as bridge_volume,
    from {{ ref("fact_usdt0_transfers") }}
    group by date
)
select
    date,
    'usdt0' as app,
    'Bridge' as category,
    bridge_dau,
    bridge_txns,
    bridge_volume,
    -- timestamp columns
    TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as created_on,
    TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as modified_on
from raw_data
where true
{{ ez_metrics_incremental('date', backfill_date) }}
and date < to_date(sysdate())
