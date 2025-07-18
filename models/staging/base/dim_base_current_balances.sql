-- depends_on: {{ ref("fact_base_address_balances_by_token") }}
{{ config(materialized="incremental", unique_key=["address", "contract_address"]) }}


{{ current_balances("base") }}
