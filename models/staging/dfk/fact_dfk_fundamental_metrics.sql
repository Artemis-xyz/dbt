{{ config(materialized="table", snowflake_warehouse="DFK") }}
{{ transfrom_avalanche_subnets_fundamental_data('dfk') }}
