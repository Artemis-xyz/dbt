{{ config(snowflake_warehouse="ETHEREUM_LG", materialized="incremental") }}
{{ token_transfer_events('ethereum') }}