{{
    config(
        materialized="incremental",
        snowflake_warehouse="AAVE",
        unique_id=["tx_hash", "event_index"],
        database="aave",
        schema="raw",
        alias="fact_v2_ethereum_liquidation_revenue",
    )
}}

{{aave_liquidation_revenue('ethereum', 'Aave V2', '0x7d2768dE32b0b80b7a3454c06BdAc94A69DDc7A9')}}