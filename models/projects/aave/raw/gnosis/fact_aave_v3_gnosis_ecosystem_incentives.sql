{{
    config(
        materialized="table",
        snowflake_warehouse="AAVE",
        database="aave",
        schema="raw",
        alias="fact_v3_gnosis_ecosystem_incentives",
    )
}}

{{ aave_v3_ecosystem_incentives('gnosis', '0xaD4F91D26254B6B0C6346b390dDA2991FDE2F20d', 'AAVE V3')}}