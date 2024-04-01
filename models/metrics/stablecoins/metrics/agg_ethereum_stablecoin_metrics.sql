-- depends_on: {{ ref('fact_ethereum_stablecoin_contracts') }}
{{ config(materialized="table") }} {{ agg_chain_stablecoin_metrics("ethereum") }}
