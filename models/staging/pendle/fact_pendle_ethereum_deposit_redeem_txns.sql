{{
    config(
        materialized="incremental",
        snowflake_warehouse="PENDLE",
        unique_key=["tx_hash", "event_index"]
    )
}}

{{ get_pendle_deposit_redeem_txns('ethereum') }}