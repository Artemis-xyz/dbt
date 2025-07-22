-- depends_on: {{ ref("fact_bsc_address_credit_by_token") }}
-- depends_on: {{ ref("fact_bsc_address_debit_by_token") }}
-- depends_on: {{ source("BALANCES", "dim_bsc_current_balances") }}
{{
    config(
        materialized="incremental",
        unique_key=["address", "contract_address", "block_timestamp"],
        snowflake_warehouse="BALANCES_XL",
    )
}}

{{ address_balances("bsc") }}
