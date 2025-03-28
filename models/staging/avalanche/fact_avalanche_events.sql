{{ config(snowflake_warehouse="AVALANCHE_MD", materialized="incremental") }}

{{ clean_flipside_evm_events('avalanche') }}
