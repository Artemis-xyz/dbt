{{
    config(
        materialized="table",
        snowflake_warehouse="AAVE",
        database="aave",
        schema="raw",
        alias="fact_v3_polygon_ecosystem_incentives",
    )
}}

{{ aave_v3_ecosystem_incentives('polygon', '0x929EC64c34a17401F460460D4B9390518E5B473e', 'AAVE V3')}}