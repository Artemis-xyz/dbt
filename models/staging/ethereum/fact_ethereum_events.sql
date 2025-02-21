{{ config(snowflake_warehouse="ETHEREUM_LG", materialized="incremental") }}

{{ clean_flipside_evm_events('ethereum') }}
