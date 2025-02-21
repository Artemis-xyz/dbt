{{ config(snowflake_warehouse="BASE_MD", materialized="incremental") }}

{{ clean_flipside_evm_events('base') }}
