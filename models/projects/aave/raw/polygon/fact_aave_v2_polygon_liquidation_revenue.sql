{{
    config(
        materialized="incremental",
        snowflake_warehouse="AAVE",
        unique_id=["tx_hash", "event_index"],
        database="aave",
        schema="raw",
        alias="fact_v2_polygon_liquidation_revenue",
    )
}}

{{aave_liquidation_revenue('polygon', 'Aave V2', '0x8dff5e27ea6b7ac08ebfdf9eb090f32ee9a30fcf')}}