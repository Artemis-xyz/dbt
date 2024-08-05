{{   
    config(
        materialized="incremental",
        snowflake_warehouse="KAICHING",
        database="KAICHING",
        schema="core",
        alias="ez_retention_metrics",
        unique_key="date"
    ) 
}}

{{get_cohort_retention_for_application("kaiching", "near")}}
