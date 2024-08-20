{{
    config(
        materialized="table",
        snowflake_warehouse="AAVE",
        database="aave",
        schema="raw",
        alias="fact_v3_bsc_deposits_borrows_lender_revenue",
    )
}}

{{ aave_deposits_borrows_lender_revenue('bsc', 'AAVE V3', '0x6807dc923806fE8Fd134338EABCA509979a7e0cB', '0x67bdF23C7fCE7C65fF7415Ba3F2520B45D6f9584', 'raw_aave_v3_bsc_rpc_data')}}