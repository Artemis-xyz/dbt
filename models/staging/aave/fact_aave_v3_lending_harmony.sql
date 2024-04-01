{{ config(materialized="table") }}
fact_aave_fork_lending("raw_aave_v3_lending_harmony", "harmony", "aave_v3")
