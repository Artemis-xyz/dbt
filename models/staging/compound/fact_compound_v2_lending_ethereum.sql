{{ config(materialized="table") }}
{{
    fact_compound_v2_fork_lending(
        "raw_compound_v2_lending_ethereum", "ethereum", "compound_v3"
    )
}}
