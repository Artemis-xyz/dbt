{{config(materialized="incremental", unique_key="token_address", snowflake_warehouse='STARGATE_MD')}}
{{
    stargate_asset_map(
        'base'
        , '0x5634c4a5FEd09819E3c46D86A965Dd9447d86e47'
        , '0xdc181Bd607330aeeBEF6ea62e03e5e1Fb4B6F7C7'
        , 'eip155:8453:native'
    )
}}
