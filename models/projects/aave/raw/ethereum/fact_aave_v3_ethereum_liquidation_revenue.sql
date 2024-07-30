{{
    config(
        materialized="incremental",
        snowflake_warehouse="AAVE",
        unique_id=["tx_hash", "event_index"],
        database="aave",
        schema="raw",
        alias="fact_v3_ethereum_liquidation_revenue",
    )
}}

{{aave_liquidation_revenue('ethereum', 'Aave V3', '0x87870Bca3F3fD6335C3F4ce8392D69350B4fA4E2')}}