{{ config(snowflake_warehouse="SEI", materialized="incremental") }}

{{ clean_flipside_evm_events('sei') }}
