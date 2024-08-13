{{
    config(
        materialized="table",
        database="aave",
        schema="raw",
        alias="fact_v3_base_deposits_borrows_lender_revenue",
    )
}}

{{ aave_deposits_borrows_lender_revenue('base', 'AAVE V3', '0xA238Dd80C259a72e81d7e4664a9801593F98d1c5', 'raw_aave_v3_base_rpc_data')}}