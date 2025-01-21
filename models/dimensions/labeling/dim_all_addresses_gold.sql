{{
    config(
        materialized="incremental",
        unique_key=["address", "chain"],
        incremental_strategy="merge",
    )
}}

SELECT * FROM {{ ref("dim_all_addresses_silver" )}} 