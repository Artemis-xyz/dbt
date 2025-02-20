{{config(materialized="incremental", unique_key=["transaction_hash", "trace_id"])}}

{{standard_8d737a18_traces("celo")}}