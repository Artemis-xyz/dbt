{{ config(materialized="table", snowflake_warehouse="AAVE") }}
{{
    fact_aave_fork_lending(
        "raw_aave_v3_avalanche_borrows_deposits_revenue", "avalanche", "aave_v3"
    )
}}
