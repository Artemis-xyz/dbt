-- depends_on: {{ ref('fact_polygon_stablecoin_contracts') }}
{{ config(materialized="table") }} {{ agg_chain_stablecoin_metrics("polygon") }}
