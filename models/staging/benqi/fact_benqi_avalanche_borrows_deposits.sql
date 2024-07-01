{{ config(materialized="table") }}
{{
    fact_compound_v2_fork_lending(
        "raw_benqi_avalanche_borrows_deposits", "avalanche", "benqi_finance"
    )
}}
