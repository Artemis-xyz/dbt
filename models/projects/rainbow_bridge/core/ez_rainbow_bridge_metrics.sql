{{
    config(
        materialized="incremental",
        snowflake_warehouse="RAINBOW_BRIDGE",
        database="rainbow_bridge",
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

with
    bridge_volume as (
        select date, bridge_volume
        from {{ ref("fact_rainbow_bridge_bridge_volume") }}
        where chain is null
    ),
    bridge_dau as (
        select date, bridge_dau
        from {{ ref("fact_rainbow_bridge_bridge_dau") }}
    )
select
    bridge_volume.date as date,
    'rainbow_bridge' as artemis_id,
    
    -- Usage Metrics
    bridge_volume.bridge_volume,
    bridge_dau.bridge_dau

    -- Timestamp columns
    , sysdate() as created_on
    , sysdate() as modified_on
from bridge_volume
left join bridge_dau on bridge_volume.date = bridge_dau.date
where true
{{ ez_metrics_incremental('bridge_volume.date', backfill_date) }}
and bridge_volume.date < to_date(sysdate())
