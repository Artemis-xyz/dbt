{{config(materialized="incremental", unique_key="token_address", snowflake_warehouse='STARGATE_MD')}}
{{
    stargate_asset_map(
        'sei'
        , '0x1502FA4be69d526124D453619276FacCab275d3D'
        , '0x5c386D85b1B82FD9Db681b9176C8a4248bb6345B'
        , 'eip155:1329:native'
    )
}}
