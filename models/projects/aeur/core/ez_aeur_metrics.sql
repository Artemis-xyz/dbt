{{
    config(
        snowflake_warehouse= "AEUR",
        database="aeur",
        schema="core",
        alias="ez_metrics",
        materialized="incremental",
        incremental_strategy="merge",
        unique_key="date",
        on_schema_change="append_new_columns",
        merge_exclude_columns=["created_on"],
        full_refresh=false
    )
}}

{% set backfill_date = None %}

{{ get_stablecoin_metrics("AEUR", breakdown='symbol', backfill_date=backfill_date) }}
