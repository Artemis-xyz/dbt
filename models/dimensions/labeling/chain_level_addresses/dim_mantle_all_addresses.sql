{{
    config(
        materialized="incremental",
        unique_key=["address"],
        incremental_strategy="merge",
    )
}}
{{ dune_evm_get_all_addresses("mantle") }}