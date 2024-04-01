-- depends_on: {{ ref("fact_arbitrum_address_credit_by_token") }}
-- depends_on: {{ ref("fact_arbitrum_address_debit_by_token") }}
-- depends_on: {{ source("BALANCES", "dim_arbitrum_current_balances") }}
{{
    config(
        materialized="incremental",
        unique_key=["address", "contract_address", "block_timestamp"],
        snowflake_warehouse="BALANCES_LG",
    )
}}

{{ address_balances("arbitrum") }}
