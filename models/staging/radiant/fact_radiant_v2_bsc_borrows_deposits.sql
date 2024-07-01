{{ config(materialized="table") }}
{{ fact_aave_fork_lending("raw_radiant_v2_bsc_borrows_deposits", "bsc", "radiant_v2") }}
