{{ config(snowflake_warehouse="ARBITRUM_MD", materialized="incremental") }}

{{ clean_flipside_evm_events('arbitrum') }}
