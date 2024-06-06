-- depends_on: {{ ref('fact_optimism_stablecoin_contracts') }}
-- depends_on: {{ ref('fact_optimism_stablecoin_transfers') }}
{{ config(materialized="table") }} {{ agg_chain_stablecoin_metrics("optimism") }}
