{{
    config(
        materialized="incremental",
        snowflake_warehouse="OPTIMISM",
        unique_key=["tx_hash", "index"],
    )
}}

{{ get_native_token_transfers('optimism') }}
