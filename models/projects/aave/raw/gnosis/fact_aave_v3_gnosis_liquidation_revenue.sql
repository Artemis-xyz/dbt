{{
    config(
        materialized="incremental",
        snowflake_warehouse="AAVE",
        unique_id=["tx_hash", "event_index"],
        database="aave",
        schema="raw",
        alias="fact_v3_gnosis_liquidation_revenue",
    )
}}

{{aave_liquidation_revenue('gnosis', 'Aave V3', '0xb50201558B00496A145fE76f7424749556E326D8')}}