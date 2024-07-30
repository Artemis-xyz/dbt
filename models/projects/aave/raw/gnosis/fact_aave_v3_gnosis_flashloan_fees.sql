{{
    config(
        materialized="table",
        snowflake_warehouse="AAVE",
        database="aave",
        schema="raw",
        alias="fact_v3_gnosis_flashloan_fees",
    )
}}


{{ flipside_lending_flashloan_fees('gnosis', 'Aave V3')}}