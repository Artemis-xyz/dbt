{{
    config(
        materialized="table",
        snowflake_warehouse="AAVE",
        database="aave",
        schema="raw",
        alias="fact_token_holders",
    )
}}

{{ token_holders('ethereum', '0x7Fc66500c84A76Ad7e9c93437bFc5Ac33E2DDaE9', '2020-09-21')}}

