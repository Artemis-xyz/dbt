{{ 
    config(
        materialized="table",
        snowflake_warehouse="near",
        database="near",
        schema="core",
        alias="ez_cohort_retention",
    )
 }}

{{ get_cohort_retention("near") }}