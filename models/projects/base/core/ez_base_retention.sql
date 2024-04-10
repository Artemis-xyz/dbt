{{ 
    config(
        materialized="table",
        snowflake_warehouse="base",
        database="base",
        schema="core",
        alias="ez_cohort_retention",
    )
 }}

{{ get_cohort_retention("base") }}