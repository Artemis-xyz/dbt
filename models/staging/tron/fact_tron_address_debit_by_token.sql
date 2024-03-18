{{
    config(
        materialized="incremental",
        unique_key=["unique_id"],
        snowflake_warehouse="BALANCES_LG",
    )
}}

{{ address_debits_allium("tron") }}
