{{ config(materialized="table") }}
{{
    fact_compound_v2_fork_lending(
        "raw_sonne_optimism_borrows_deposits", "optimism", "sonne_finance"
    )
}}
