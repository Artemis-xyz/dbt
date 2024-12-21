{{
    config(
        materialized='table',
        snowflake_warehouse='ENZYME',
        database='ENZYME',
        schema='raw',
        alias='fact_enzyme_token_holders'
    )
}}

{{ token_holders('ethereum', '0xec67005c4e498ec7f55e092bd1d35cbc47c91892', '2017-02-21') }}