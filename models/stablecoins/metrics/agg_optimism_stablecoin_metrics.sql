-- depends_on: {{ ref('fact_optimism_stablecoin_contracts') }}
{{ config(materialized="table") }} {{ agg_chain_stablecoin_metrics("optimism") }}
