{{config(materialized="incremental", unique_key=["tx_hash", "event_index"], snowflake_warehouse='STARGATE')}}
{{stargate_OFTReceived('sei')}}
