{{ config(materialized="table", snowflake_warehouse="BENQI_FINANCE") }}
{{
    fact_compound_v2_fork_lending(
        "raw_benqi_avalanche_borrows_deposits", "avalanche", "benqi_finance"
    )
}}
