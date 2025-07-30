{{   
    config(
        materialized="incremental",
        snowflake_warehouse="KAICHING",
        database="KAICHING",
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

with 
    fundamental_data as ({{get_bam_data_for_application("kaiching", ["near"])}}),
    rolling_wau_mau as ({{get_rolling_active_address_metrics_by_app("kaiching", "near")}})
SELECT 
    fd.date,
    fd.app,
    fd.friendly_name,
    fd.gas_usd,
    fd.txns,
    fd.daa,
    fd.new_users,
    fd.returning_users,
    rwa.mau,
    rwa.wau,
    -- timestamp columns
    TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as created_on,
    TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as modified_on
FROM 
    fundamental_data as fd
left join rolling_wau_mau as rwa on fd.date = rwa.date
where true
{{ ez_metrics_incremental('fd.date', backfill_date) }}
and fd.date < to_date(sysdate())
