{{
    config(
        materialized="incremental",
        snowflake_warehouse="PENDLE",
        unique_key=["block_timestamp", "tx_hash", "event_index"]
    )
}}

{{ get_pendle_deposit_redeem_txns('arbitrum') }}
