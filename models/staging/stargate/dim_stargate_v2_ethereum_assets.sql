{{config(materialized="incremental", unique_key="token_address", snowflake_warehouse='STARGATE_MD')}}

{{
    stargate_asset_map(
        'ethereum'
        , '0x6d6620eFa72948C5f68A3C8646d58C00d3f4A980'
        , '0x77b2043768d28e9c9ab44e1abfc95944bce57931'
        , 'eip155:1:native'
    )
}}
