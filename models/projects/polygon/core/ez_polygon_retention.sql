{{ 
    config(
        materialized="table",
        snowflake_warehouse="polygon",
        database="polygon",
        schema="core",
        alias="ez_cohort_retention",
    )
 }}

{{ get_cohort_retention("polygon") }}