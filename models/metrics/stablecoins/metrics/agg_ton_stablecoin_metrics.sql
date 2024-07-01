-- depends_on: {{ ref('fact_ton_stablecoin_contracts') }}
-- depends_on: {{ ref('ez_ton_stablecoin_transfers') }}
{{ config(materialized="table") }} {{ agg_chain_stablecoin_metrics("ton") }}
