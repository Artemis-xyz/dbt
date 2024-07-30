{{
    config(
        materialized="incremental",
        unique_id=["tx_hash", "event_index"],
        snowflake_warehouse="AAVE",
        database="aave",
        schema="raw",
        alias="fact_v3_bsc_liquidation_revenue",
    )
}}

{{aave_liquidation_revenue('bsc', 'Aave V3', '0x6807dc923806fE8Fd134338EABCA509979a7e0cB')}}