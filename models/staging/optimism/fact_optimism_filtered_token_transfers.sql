{{
    config(
        materialized="incremental",
        snowflake_warehouse="OPTIMISM",
        unique_key=["tx_hash", "index"],
    )
}}

{{ get_token_transfer_filtered('optimism') }}
