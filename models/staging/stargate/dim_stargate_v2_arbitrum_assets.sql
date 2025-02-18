{{config(materialized="table", snowflake_warehouse='STARGATE')}}
{{
    stargate_asset_map(
        'arbitrum'
        , '0x19cFCE47eD54a88614648DC3f19A5980097007dD'
        , '0xA45B5130f36CDcA45667738e2a258AB09f4A5f7F'
        , '0x82aF49447D8a07e3bd95BD0d56f35241523fBab1'
    )
}}
