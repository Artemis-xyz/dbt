{{config(materialized="incremental", unique_key="token_address", snowflake_warehouse='STARGATE_MD')}}
{{
    stargate_asset_map(
        'arbitrum'
        , '0x19cFCE47eD54a88614648DC3f19A5980097007dD'
        , '0xA45B5130f36CDcA45667738e2a258AB09f4A5f7F'
        , 'eip155:42161:native'
    )
}}
