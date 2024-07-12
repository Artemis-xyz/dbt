{{ config(materialized="table", snowflake_warehouse="BEAM") }}
{{ transform_avalanche_subnets_fundamental_data('beam') }}
