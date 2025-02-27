{{config(materialized="incremental", snowflake_warehouse='STARGATE_MD', unique_key=["tx_hash", "event_index"])}}
{{stargate_OFTSent('optimism')}}