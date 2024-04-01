-- depends_on: {{ ref('fact_tron_stablecoin_contracts') }}
{{ config(materialized="table") }} {{ agg_chain_stablecoin_metrics("tron") }}
