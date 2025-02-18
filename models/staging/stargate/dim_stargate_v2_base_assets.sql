{{config(materialized="table", snowflake_warehouse='STARGATE')}}
{{
    stargate_asset_map(
        'base'
        , '0x5634c4a5FEd09819E3c46D86A965Dd9447d86e47'
        , '0xdc181Bd607330aeeBEF6ea62e03e5e1Fb4B6F7C7'
        , '0x4200000000000000000000000000000000000006'
    )
}}
