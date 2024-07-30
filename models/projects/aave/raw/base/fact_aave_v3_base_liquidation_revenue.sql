{{
    config(
        materialized="incremental",
        snowflake_warehouse="AAVE",
        unique_id=["tx_hash", "event_index"],
        database="aave",
        schema="raw",
        alias="fact_v3_base_liquidation_revenue",
    )
}}

{{aave_liquidation_revenue('base', 'Aave V3', '0xA238Dd80C259a72e81d7e4664a9801593F98d1c5')}}