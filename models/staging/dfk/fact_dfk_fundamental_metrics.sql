{{ config(materialized="table") }}
{{ transform_avalanche_subnets_fundamental_data('dfk') }}
