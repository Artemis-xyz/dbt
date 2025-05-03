-- depends_on: {{ ref("fact_celo_address_credit_by_token") }}
-- depends_on: {{ ref("fact_celo_address_debit_by_token") }}
-- depends_on: {{ source("BALANCES", "dim_celo_current_balances") }}
{{
    config(
        materialized="incremental",
        unique_key=["address", "contract_address", "block_timestamp"],
        snowflake_warehouse="CELO_LG",
    )
}}

{{ address_balances("celo") }}