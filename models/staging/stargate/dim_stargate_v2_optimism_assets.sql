{{config(materialized="incremental", unique_key="token_address", snowflake_warehouse='STARGATE_MD')}}
{{
    stargate_asset_map(
        'optimism'
        , '0xF1fCb4CBd57B67d683972A59B6a7b1e2E8Bf27E6'
        , '0xe8CDF27AcD73a434D661C84887215F7598e7d0d3'
        , 'eip155:10:native'
    )
}}
