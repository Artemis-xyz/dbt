{{ config(snowflake_warehouse="ETHEREUM_LG", materialized="incremental") }}

{{ decode_goldsky_events('ethereum') }}