{{ config(snowflake_warehouse="EULER", materialized="incremental", unique_key=["transaction_hash", "event_index"], enabled=false) }}

{{ euler_ProxyCreated("bob", "0x046a9837A61d6b6263f54F4E27EE072bA4bdC7e4") }}