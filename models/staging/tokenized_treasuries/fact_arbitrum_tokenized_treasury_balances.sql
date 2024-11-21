{{
    config(
        materialized="incremental",
        unique_key="unique_id",
        snowflake_warehouse="ONDO",
    )
}}

{{tokenized_treasury_balances("arbitrum")}}