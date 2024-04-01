{{ config(materialized="table") }}
{{ fact_aave_fork_lending("raw_spark_ethereum_borrows_deposits", "ethereum", "spark") }}
