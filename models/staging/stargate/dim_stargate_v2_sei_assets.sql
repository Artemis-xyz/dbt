{{config(materialized="incremental", unique_key="token_address", snowflake_warehouse='STARGATE_MD')}}
{{
    stargate_asset_map(
        'sei'
        , '0x1502FA4be69d526124D453619276FacCab275d3D'
        , '0x5c386D85b1B82FD9Db681b9176C8a4248bb6345B'
        , '0x160345fC359604fC6e70E3c5fAcbdE5F7A9342d8'
    )
}}
