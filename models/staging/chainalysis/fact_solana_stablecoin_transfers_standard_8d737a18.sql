{{config(materialized="incremental", unique_key=["tx_hash", "event_index"])}}


{{standard_8d737a18_stablecoin_transfers("solana")}}