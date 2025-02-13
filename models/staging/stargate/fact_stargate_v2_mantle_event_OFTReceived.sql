{{config(materialized="table", unique_key=["tx_hash", "event_index"])}}
{{stargate_OFTReceived('mantle')}}
