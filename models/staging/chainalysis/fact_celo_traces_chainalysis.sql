{{config(materialized="incremental", unique_key=["tx_hash", "event_index"])}}

{{chainalaysis_traces("celo")}}