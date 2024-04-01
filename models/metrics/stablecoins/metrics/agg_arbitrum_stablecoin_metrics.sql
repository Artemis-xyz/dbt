-- depends_on: {{ ref('fact_arbitrum_stablecoin_contracts') }}
{{ config(materialized="table") }} {{ agg_chain_stablecoin_metrics("arbitrum") }}
