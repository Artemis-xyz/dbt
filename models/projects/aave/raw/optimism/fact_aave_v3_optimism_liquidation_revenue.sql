{{
    config(
        materialized="incremental",
        snowflake_warehouse="AAVE",
        unique_id=["tx_hash", "event_index"],
        database="aave",
        schema="raw",
        alias="fact_v3_optimism_liquidation_revenue",
    )
}}

{{aave_liquidation_revenue('optimism', 'Aave V3', '0x794a61358D6845594F94dc1DB02A252b5b4814aD')}}