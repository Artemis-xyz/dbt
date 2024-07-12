{{ config(materialized="table", snowflake_warehouse="DEXALOT") }}
{{ transform_avalanche_subnets_fundamental_data('dexalot') }}
