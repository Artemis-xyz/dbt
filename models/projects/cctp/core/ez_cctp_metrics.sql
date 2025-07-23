{{
    config(
        materialized="incremental",
        snowflake_warehouse="CCTP",
        database="cctp",
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

-- NOTE: When running a backfill, add merge_update_columns=[<columns>] to the config and set the backfill date below

{% set backfill_date = var("backfill_date", None) %}

with
    bridge_volume as (
        select date, bridge_volume
        from {{ ref("fact_cctp_bridge_volume") }}
        {{ ez_metrics_incremental('date', backfill_date) }}
            and chain is null
    ),
    bridge_dau as (
        select date, bridge_dau
        from {{ ref("fact_cctp_bridge_dau") }}
        {{ ez_metrics_incremental('date', backfill_date) }}
    )
select
    bridge_volume.date as date,
    'cctp' as protocol,
    'Bridge' as category,
    bridge_volume.bridge_volume,
    bridge_dau.bridge_dau
    -- timestamp columns
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as created_on
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as modified_on
from bridge_volume
left join bridge_dau on bridge_volume.date = bridge_dau.date
{{ ez_metrics_incremental('bridge_volume.date', backfill_date) }}
    and bridge_volume.date < to_date(sysdate())
