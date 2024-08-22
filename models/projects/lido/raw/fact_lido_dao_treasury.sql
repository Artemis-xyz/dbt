{{
    config(
        materialized="table",
        snowflake_warehouse="LIDO",
        database="lido",
        schema="raw",
        alias="fact_lido_dao_treasury",
    )
}}

{{ get_treasury_balance('ethereum', '0x3e40d73eb977dc6a537af587d48316fee66e9c8c', '2020-12-17')}}