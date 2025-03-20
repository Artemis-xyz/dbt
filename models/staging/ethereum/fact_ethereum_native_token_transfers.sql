{{
    config(
        materialized="incremental",
        unique_key=["transaction_hash", "index"],
        snowflake_warehouse="ETHEREUM_LG"
    )
}}

{{ clean_flipside_evm_transactions('ethereum') }}