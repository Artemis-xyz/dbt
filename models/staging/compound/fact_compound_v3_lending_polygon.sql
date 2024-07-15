{{ config(materialized="table") }}
{{
    fact_compound_v3_fork_lending(
        "raw_compound_v3_lending_polygon", "polygon", "compound_v3"
    )
}}
