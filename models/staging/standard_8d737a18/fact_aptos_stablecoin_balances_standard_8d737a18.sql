{{ config(materialized="incremental", snowflake_warehouse="STANDARD_8D737A18", unique_key=["unique_id"]) }}

{{ standard_8d737a18_stablecoin_balances('aptos') }}
