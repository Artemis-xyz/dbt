{{ config(snowflake_warehouse="EULER", materialized="incremental", unique_key=["transaction_hash", "event_index"], enabled=false) }}

{{ euler_ProxyCreated("sonic", "0xF075cC8660B51D0b8a4474e3f47eDAC5fA034cFB") }}