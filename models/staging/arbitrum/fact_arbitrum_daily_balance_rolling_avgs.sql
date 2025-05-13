{{
    config(
        materialized="incremental",
        unique_key=["date", "address"],
        snowflake_warehouse="BALANCES_MD",
    )
}}

{{ daily_address_balance_rolling_avgs("arbitrum") }}
