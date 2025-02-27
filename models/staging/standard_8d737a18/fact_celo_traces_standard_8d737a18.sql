{{config(materialized="incremental", snowflake_warehouse="STANDARD_8D737A18", unique_key=["transaction_hash", "trace_id"])}}

{{standard_8d737a18_traces("celo")}}