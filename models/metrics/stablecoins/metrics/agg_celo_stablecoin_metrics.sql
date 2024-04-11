-- depends_on: {{ ref('fact_celo_stablecoin_contracts') }}
{{ config(materialized="table") }} {{ agg_chain_stablecoin_metrics("celo") }}
