{{ config(materialized="table") }}
{{ fact_aave_fork_lending("raw_spark_gnosis_borrows_deposits", "gnosis", "spark") }}
