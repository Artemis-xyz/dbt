-- depends_on: {{ ref('fact_avalanche_stablecoin_contracts') }}
{{ config(materialized="table") }} {{ agg_chain_stablecoin_metrics("avalanche") }}
