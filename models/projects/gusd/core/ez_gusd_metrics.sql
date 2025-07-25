{{
    config(
        materialized="incremental",
        snowflake_warehouse= "GUSD",
        database="gusd",
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

{{ get_stablecoin_metrics("GUSD", breakdown='symbol', backfill_date=backfill_date) }}
