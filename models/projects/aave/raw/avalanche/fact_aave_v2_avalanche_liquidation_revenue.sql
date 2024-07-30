{{
    config(
        materialized="incremental",
        snowflake_warehouse="AAVE",
        unique_id=["tx_hash", "event_index"],
        database="aave",
        schema="raw",
        alias="fact_v2_avalanche_liquidation_revenue",
    )
}}

{{aave_liquidation_revenue('avalanche', 'Aave V2', '0x4F01AeD16D97E3aB5ab2B501154DC9bb0F1A5A2C')}}