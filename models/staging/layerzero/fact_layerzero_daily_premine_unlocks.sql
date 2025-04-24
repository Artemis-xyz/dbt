{{
    config(
        materialized="table",
        snowflake_warehouse="LAYERZERO"
    )
}}

select * from {{source('MANUAL_STATIC_TABLES', 'layerzero_daily_premine_unlocks')}}