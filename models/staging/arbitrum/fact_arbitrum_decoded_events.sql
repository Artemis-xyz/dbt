{{ config(snowflake_warehouse="ARBITRUM_LG", materialized="incremental") }}

{{ decode_artemis_events('arbitrum') }}
