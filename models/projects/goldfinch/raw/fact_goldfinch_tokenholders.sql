{{
    config(
        materialized = 'table',
        snowflake_warehouse = 'GOLDFINCH',
        database = 'goldfinch',
        schema = 'raw',
        alias = 'fact_goldfinch_tokenholders'
    )
}}

{{ token_holders('ethereum', '0xdab396ccf3d84cf2d07c4454e10c8a6f5b008d2b', '2021-10-22')}}
