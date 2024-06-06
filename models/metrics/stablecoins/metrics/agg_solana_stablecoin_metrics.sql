-- depends_on: {{ ref('fact_solana_stablecoin_contracts') }}
-- depends_on: {{ ref('fact_solana_stablecoin_premint_addresses') }}
-- depends_on: {{ ref('fact_solana_stablecoin_transfers') }}
{{ config(materialized="table", snowflake_warehouse="STABLECOIN_LG") }}
{{ agg_chain_stablecoin_metrics("solana") }}
