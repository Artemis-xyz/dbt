{{ config(materialized="table") }}
{{
    fact_compound_v3_fork_lending(
        "raw_compound_v3_lending_arbitrum", "arbitrum", "compound_v3"
    )
}}
