{{
    config(
        materialized="table",
        snowflake_warehouse="POLYGON_SM",
    )
}}

{{ filter_p2p_token_transfers("polygon", 200, 
    blacklist=(
        "0xaaa5b9e6c589642f98a1cda99b9d024b8407285a",
        "0x229b1b6c23ff8953d663c4cbb519717e323a0a84",
        "0xef938b6da8576a896f6e0321ef80996f4890f9c4",
        "0x228b5c21ac00155cf62c57bcc704c0da8187950b"
    )
)}}