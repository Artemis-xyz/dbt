{{ config(materialized="table", snowflake_warehouse="UWULEND") }}
{{
    fact_aave_fork_lending(
        "raw_uwulend_ethereum_borrows_deposits", "ethereum", "uwulend"
    )
}}
