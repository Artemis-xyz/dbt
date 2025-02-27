{{config(materialized="incremental", unique_key="token_address", snowflake_warehouse='STARGATE_MD')}}
{{
    stargate_asset_map(
        'bsc'
        , '0x6E3d884C96d640526F273C61dfcF08915eBd7e2B'
    )
}}
