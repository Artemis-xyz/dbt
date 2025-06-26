{{ config(snowflake_warehouse="BALANCES_LG", materialized="incremental", unique_key=["block_timestamp", "block_number", "contract_address", "address"])}}

{{ evm_address_balances("tron") }}