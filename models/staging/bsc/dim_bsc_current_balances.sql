-- depends_on: {{ ref("fact_bsc_address_balances_by_token") }}
{{
    config(
        materialized="incremental",
        unique_key=["address", "contract_address"],
        snowflake_warehouse="BALANCES_MD",
    )
}}


{{ current_balances("bsc") }}
