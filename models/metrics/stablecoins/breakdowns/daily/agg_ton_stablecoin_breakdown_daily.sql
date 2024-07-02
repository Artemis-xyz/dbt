-- depends_on: {{ ref('fact_ton_stablecoin_contracts') }}
-- depends_on: {{ ref('ez_ton_address_balances_by_token') }}
-- depends_on: {{ ref('ez_ton_stablecoin_transfers') }}
{{ config(materialized="table", snowflake_warehouse="TON_MD") }}
{{ agg_chain_stablecoin_breakdown_ton("day") }}
