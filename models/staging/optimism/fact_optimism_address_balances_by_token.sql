-- depends_on: {{ ref("fact_optimism_address_credit_by_token") }}
-- depends_on: {{ ref("fact_optimism_address_debit_by_token") }}
-- depends_on: {{ source("BALANCES", "dim_optimism_current_balances") }}
{{
    config(
        materialized="incremental",
        unique_key=["address", "contract_address", "block_timestamp"],
        snowflake_warehouse="BALANCES_LG",
    )
}}

{{ address_balances("optimism") }}
