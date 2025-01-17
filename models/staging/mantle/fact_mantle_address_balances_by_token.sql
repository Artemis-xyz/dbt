-- depends_on: {{ ref("fact_mantle_address_credit_by_token") }}
-- depends_on: {{ ref("fact_mantle_address_debit_by_token") }}
-- depends_on: {{ source("BALANCES", "dim_mantle_current_balances") }}
{{
    config(
        materialized="incremental",
        unique_key=["address", "contract_address", "block_timestamp"],
        snowflake_warehouse="MANTLE",
    )
}}

{{ address_balances("mantle") }}