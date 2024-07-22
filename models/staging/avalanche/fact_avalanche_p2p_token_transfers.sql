{{
    config(
        materialized="table",
    )
}}

{{ 
    filter_p2p_token_transfers(
        "avalanche",
        blacklist=(
            "0x73e7e73447d1bafcb7e6e03416046654ae8b7c20",
            "0x9f285507ea5b4f33822ca7abb5ec8953ce37a645"
        )
    ) 
}}