{{ config(materialized="table", snowflake_warehouse="COMPOUND") }}
{{
    fact_compound_v3_fork_lending(
        "raw_compound_v3_lending_ethereum", "ethereum", "compound_v3"
    )
}}
