{{ config(materialized="table", snowflake_warehouse="BEAM") }}
{{ transfrom_avalanche_subnets_fundamental_data('beam') }}
