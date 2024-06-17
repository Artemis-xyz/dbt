{{ config(materialized="table", snowflake_warehouse="MOONWELL") }}
{{
    fact_compound_v2_fork_lending(
        "raw_moonwell_base_borrows_deposits", "base", "moonwell"
    )
}}
