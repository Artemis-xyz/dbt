
{{ config(snowflake_warehouse="BASE", materialized="incremental") }}
{{ token_transfer_events('base') }}
