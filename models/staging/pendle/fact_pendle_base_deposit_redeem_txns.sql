{{
    config(
        materialized="incremental",
        snowflake_warehouse="PENDLE"
    )
}}

{{ get_pendle_deposit_redeem_txns('base') }}
