{{ config(snowflake_warehouse="EULER", materialized="incremental", unique_key=["transaction_hash", "event_index"]) }}

{{ euler_ProxyCreated("avalanche", "0xaf4B4c18B17F6a2B32F6c398a3910bdCD7f26181") }}