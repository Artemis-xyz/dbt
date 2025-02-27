{{config(materialized="incremental", snowflake_warehouse="STANDARD_8D737A18", unique_key=["transaction_hash", "transfer_index"])}}


{{standard_8d737a18_stablecoin_transfers("solana")}}