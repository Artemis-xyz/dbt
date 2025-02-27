{{config(materialized="incremental", unique_key="token_address", snowflake_warehouse='STARGATE_MD')}}
{{
    stargate_asset_map(
        'polygon'
        , '0x6CE9bf8CDaB780416AD1fd87b318A077D2f50EaC'
    )
}}
