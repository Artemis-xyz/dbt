-- depends_on: {{ ref('fact_solana_stablecoin_contracts') }}
-- depends_on: {{ ref('fact_solana_address_balances_by_token') }}
{{ config(materialized="table", snowflake_warehouse="STABLECOIN_LG_2") }}
{{ agg_chain_stablecoin_breakdown("solana", "month") }}
