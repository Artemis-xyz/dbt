-- depends_on: {{ ref('fact_blast_stablecoin_contracts') }}
{{ config(materialized="table") }} {{ agg_chain_stablecoin_metrics("blast") }}
