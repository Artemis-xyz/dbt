{{
    config(
        materialized='table',
        snowflake_warehouse='SYNTHETIX',
        database='SYNTHETIX',
        schema='raw',
        alias='fact_synthetix_treasury_by_token'
    )
}}

{{ get_treasury_balance(
        chain='ethereum',
        addresses=[
            '0xd939611c3CA425B4f6d4a82591EaB3da43C2f4a0',
            '0x99F4176EE457afedFfCB1839c7aB7A030a5e4A92',
            '0x547d28cDd6A69e3366d6aE3EC39543F09Bd09417',
            '0x579b66d0A7C48eEe63B3BD2bcA17bf0Fa0F0787b', 
            '0x7b955E5CA4d0D65e91e8d945Af5696d5F0707Bec', 
            '0xB91ef9a2c37C20803EfD33d98F09296d2468403b'
        ],
        earliest_date='2018-03-30'
    )
}}