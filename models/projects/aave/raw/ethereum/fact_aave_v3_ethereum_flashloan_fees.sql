{{
    config(
        materialized="table",
        snowflake_warehouse="AAVE",
        database="aave",
        schema="raw",
        alias="fact_v3_ethereum_flashloan_fees",
    )
}}


{{ flipside_lending_flashloan_fees('ethereum', 'Aave V3')}}