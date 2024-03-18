{{ config(materialized="table") }}
{{ fact_compound_v2_fork_lending("raw_venus_v4_lending_bsc", "bsc", "venus_v4") }}
