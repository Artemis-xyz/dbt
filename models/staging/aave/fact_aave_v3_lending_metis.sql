{{ config(materialized="table") }}
{{
    fact_aave_fork_lending(
        "raw_aave_v3_metis_borrows_deposits_revenue", "metis", "aave_v3"
    )
}}
