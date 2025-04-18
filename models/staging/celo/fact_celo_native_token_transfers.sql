{{ 
    config(
        materialized="incremental",
        unique_key=["transaction_hash", "trace_index"],
        snowflake_warehouse="CELO_LG"
    )
}}

{{ clean_goldsky_evm_native_token_transfers("celo", "celo") }}
