{{
    config(
        materialized="incremental",
        snowflake_warehouse= "BOLD",
        database="bold",
        schema="core",
        alias="ez_metrics",
        incremental_strategy="merge",
        unique_key="date",
        on_schema_change="append_new_columns",
        merge_exclude_columns=["created_on"],
        full_refresh=false
    )
}}

-- NOTE: When running a backfill, add merge_update_columns=[<columns>] to the config and set the backfill date below

{% set backfill_date = None %}

{{ get_stablecoin_metrics("BOLD", breakdown='symbol', backfill_date=backfill_date) }}
