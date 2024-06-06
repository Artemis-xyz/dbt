-- depends_on: {{ ref('fact_tron_stablecoin_contracts') }}
-- depends_on: {{ ref('fact_tron_stablecoin_transfers') }}
{{ config(materialized="table") }} {{ agg_chain_stablecoin_metrics("tron") }}
