{{ config(snowflake_warehouse="AVALANCHE_LG", materialized="incremental") }}

{{ decode_artemis_events('arbitrum') }}
