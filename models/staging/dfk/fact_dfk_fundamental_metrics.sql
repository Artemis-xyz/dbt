{{ config(materialized="table", snowflake_warehouse="DFK") }}
{{ transform_avalanche_subnets_fundamental_data('dfk') }}
