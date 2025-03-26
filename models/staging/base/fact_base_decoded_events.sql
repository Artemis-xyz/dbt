{{ config(snowflake_warehouse="BASE_LG", materialized="incremental") }}

{{ decode_artemis_events('base') }}
