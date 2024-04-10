{{ 
    config(
        materialized="table",
        snowflake_warehouse="tron",
        database="tron",
        schema="core",
        alias="ez_cohort_retention",
    )
 }}

{{ get_cohort_retention("tron") }}