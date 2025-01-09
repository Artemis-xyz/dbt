{{
    config(
        materialized="incremental",
        unique_key=["address"],
        incremental_strategy="merge",
    )
}}
{{ evm_get_all_addresses("optimism") }}