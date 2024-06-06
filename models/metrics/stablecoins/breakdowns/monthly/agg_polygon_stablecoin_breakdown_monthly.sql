-- depends_on: {{ ref('fact_polygon_stablecoin_contracts') }}
-- depends_on: {{ ref('fact_polygon_address_balances_by_token') }}
-- depends_on: {{ ref('fact_polygon_stablecoin_transfers') }}
{{ config(materialized="table", snowflake_warehouse="STABLECOIN_LG_2") }}
{{ agg_chain_stablecoin_breakdown("polygon", "month") }}
