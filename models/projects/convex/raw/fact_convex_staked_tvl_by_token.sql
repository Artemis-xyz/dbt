{{
    config(
        materialized = 'table',
        snowflake_warehouse = 'CONVEX',
        database = 'CONVEX',
        schema = 'raw',
        alias = 'fact_convex_staked_tvl_by_token'
    )
}}

{{ get_treasury_balance(
        chain='ethereum',
        addresses=[
            '0x989aeb4d175e16225e39e87d0d97a3360524ad80',
            '0x59cfcd384746ec3035299d90782be065e466800b',
            '0xEC6B8A3F3605B083F7044C0F31f2cac0caf1d469'
        ],
        earliest_date='2020-09-12'
    )
}}