{{
    config(
        materialized="table",
        snowflake_warehouse="AAVE",
        database="aave",
        schema="raw",
        alias="fact_v3_optimism_flashloan_fees",
    )
}}


{{ flipside_lending_flashloan_fees('optimism', 'Aave V3')}}