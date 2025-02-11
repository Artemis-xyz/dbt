{{config(materialized="incremental", unique_key=["tx_hash", "event_index"])}}


{{chainanalysis_stablecoin_transfers("solana")}}