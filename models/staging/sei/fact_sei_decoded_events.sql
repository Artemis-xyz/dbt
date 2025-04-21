{{ config(snowflake_warehouse="SEI", materialized="incremental") }}

{{ decode_artemis_events('sei') }}
