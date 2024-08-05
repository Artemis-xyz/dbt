{{
    config(
        materialized="table",
        snowflake_warehouse="AAVE",
        database="aave",
        schema="raw",
        alias="fact_v3_ethereum_ecosystem_incentives",
    )
}}

{{ aave_v3_ecosystem_incentives('ethereum', '0x8164Cc65827dcFe994AB23944CBC90e0aa80bFcb', 'AAVE V3')}}