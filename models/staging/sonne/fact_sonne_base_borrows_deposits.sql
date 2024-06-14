{{ config(materialized="table", snowflake_warehouse="SONNE") }}
{{
    fact_compound_v2_fork_lending(
        "raw_sonne_base_borrows_deposits", "base", "sonne_finance"
    )
}}
