{{
    config(
        materialized="table",
        snowflake_warehouse="AAVE",
        database="aave",
        schema="raw",
        alias="fact_v3_base_ecosystem_incentives",
    )
}}

{{ aave_v3_ecosystem_incentives('base', '0xf9cc4F0D883F1a1eb2c253bdb46c254Ca51E1F44', 'AAVE V3')}}