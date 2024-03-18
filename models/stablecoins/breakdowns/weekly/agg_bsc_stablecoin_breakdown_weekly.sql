-- depends_on: {{ ref('fact_bsc_stablecoin_contracts') }}
-- depends_on: {{ ref('fact_bsc_address_balances_by_token') }}
{{ config(materialized="table", snowflake_warehouse="STABLECOIN_LG_2") }}
{{ agg_chain_stablecoin_breakdown("bsc", "week") }}
