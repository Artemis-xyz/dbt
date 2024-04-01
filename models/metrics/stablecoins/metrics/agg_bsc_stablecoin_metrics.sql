-- depends_on: {{ ref('fact_bsc_stablecoin_contracts') }}
{{ config(materialized="table") }} {{ agg_chain_stablecoin_metrics("bsc") }}
