{{ config(materialized="table") }}
{{
    fact_aave_fork_lending(
        "raw_aave_v2_avalanche_borrows_deposits_revenue", "avalanche", "aave_v2"
    )
}}
