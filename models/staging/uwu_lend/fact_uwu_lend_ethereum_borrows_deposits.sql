{{ config(materialized="table") }}
{{
    fact_aave_fork_lending(
        "raw_uwulend_ethereum_borrows_deposits", "ethereum", "uwulend"
    )
}}
