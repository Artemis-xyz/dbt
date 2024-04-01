{{ config(materialized="table") }}
{{
    fact_aave_fork_lending(
        "raw_radiant_v2_ethereum_borrows_deposits", "ethereum", "radiant_v2"
    )
}}
