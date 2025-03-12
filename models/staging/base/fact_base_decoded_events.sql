{{ config(snowflake_warehouse="ETHEREUM_LG", materialized="incremental") }}

{{ decode_artemis_events('base') }}
