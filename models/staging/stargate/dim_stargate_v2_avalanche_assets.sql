{{config(materialized="incremental", unique_key="token_address", snowflake_warehouse='STARGATE_MD')}}
{{
    stargate_asset_map(
        'avalanche'
        , '0x17E450Be3Ba9557F2378E20d64AD417E59Ef9A34'
    )
}}
