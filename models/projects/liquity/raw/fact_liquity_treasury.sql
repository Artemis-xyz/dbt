{{
    config(
        materialized='table',
        snowflake_warehouse='LIQUITY',
        database='LIQUITY',
        schema='raw',
        alias='fact_liquity_treasury'
    )
}}
-- returns date, chain, contract_address, token, native_balance, usd_balance
{{ get_treasury_balance('ethereum', '0xF06016D822943C42e3Cb7FC3a6A3B1889C1045f8', '2021-03-17') }}