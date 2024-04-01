-- depends_on: {{ ref('fact_base_stablecoin_contracts') }}
{{ config(materialized="table") }} {{ agg_chain_stablecoin_metrics("base") }}
