{{
    config(
        materialized="table",
        snowflake_warehouse="AAVE",
        database="aave",
        schema="raw",
        alias="fact_v2_avalanche_ecosystem_incentives",
    )
}}

{{ aave_v2_ecosystem_incentives('avalanche', '0x01D83Fe6A10D2f2B7AF17034343746188272cAc9', 'AAVE V2')}}