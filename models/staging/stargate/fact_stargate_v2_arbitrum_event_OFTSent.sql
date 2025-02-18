{{config(materialized="incremental", unique_key=["tx_hash", "event_index"])}}
{{stargate_OFTSent('arbitrum')}}