{{   
    config(
        materialized="incremental",
        snowflake_warehouse="KAICHING",
        database="KAICHING",
        schema="core",
        alias="ez_metrics",
        unique_key="date"
    ) 
}}

with 
    fundamental_data as ({{get_bam_data_for_application("kaiching", ["near"])}}),
    rolling_wau_mau as ({{get_rolling_active_address_metrics_by_app("kaiching", "near")}})
SELECT 
    fd.date
    , fd.app
    , fd.friendly_name
    , fd.gas_usd
    , fd.txns
    , fd.daa
    , fd.new_users
    , fd.returning_users
    , rwa.mau
    , rwa.wau
FROM 
    fundamental_data as fd
left join rolling_wau_mau as rwa on fd.date = rwa.date
