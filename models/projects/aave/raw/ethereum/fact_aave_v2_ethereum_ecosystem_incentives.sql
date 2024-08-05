{{
    config(
        materialized="table",
        snowflake_warehouse="AAVE",
        database="aave",
        schema="raw",
        alias="fact_v2_ethereum_ecosystem_incentives",
    )
}}

{{ aave_v2_ecosystem_incentives('ethereum', '0xd784927Ff2f95ba542BfC824c8a8a98F3495f6b5', 'AAVE V2')}}