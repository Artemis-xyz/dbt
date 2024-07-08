{{ config(materialized="table", snowflake_warehouse="DEXALOT") }}
{{ transfrom_avalanche_subnets_fundamental_data('dexalot') }}
