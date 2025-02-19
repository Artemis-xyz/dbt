{{config(materialized="incremental", unique_key=["tx_hash", "trace_id"])}}

{{standard_8d737a18_traces("celo")}}