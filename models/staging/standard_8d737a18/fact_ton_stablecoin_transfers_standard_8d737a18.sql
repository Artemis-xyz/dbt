{{config(materialized="incremental", unique_key=["transaction_hash", "transfer_index"])}}


{{standard_8d737a18_stablecoin_transfers("ton")}}