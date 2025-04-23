{{
    config(
        materialized="incremental",
        unique_key="transaction_hash",
        snowflake_warehouse="CELO_LG"
    )
}}

{{ clean_goldsky_transactions("celo") }}