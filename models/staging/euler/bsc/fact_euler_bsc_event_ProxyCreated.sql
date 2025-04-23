{{ config(snowflake_warehouse="EULER", materialized="incremental", unique_key=["transaction_hash", "event_index"]) }}

{{ euler_ProxyCreated("bsc", "0x7F53E2755eB3c43824E162F7F6F087832B9C9Df6") }}