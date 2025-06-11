-- depends_on: {{ ref("fact_blast_address_balances_by_token") }}
{{ config(materialized="incremental", unique_key=["address", "contract_address"], enabled=false) }}


{{ current_balances("blast") }}
