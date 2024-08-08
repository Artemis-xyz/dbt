{{
    config(
        materialized="incremental",
        snowflake_warehouse="MEDIUM",
        unique_key=["tx_hash", "index"],
    )
}}

{{ get_token_transfer_filtered('optimism') }}
