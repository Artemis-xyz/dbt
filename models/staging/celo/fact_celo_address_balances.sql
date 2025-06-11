
{{
    config(
        materialized="incremental",
        unique_key=["address", "contract_address", "block_timestamp"],
        snowflake_warehouse="CELO_LG",
    )
}}

{{ evm_address_balances("celo") }}