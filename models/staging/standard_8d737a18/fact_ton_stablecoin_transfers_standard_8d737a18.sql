{{config(materialized="incremental", unique_key=["tx_hash", "event_index"], snowflake_warehouse='ANALYTICS_XL')}}


{{standard_8d737a18_stablecoin_transfers("ton")}}