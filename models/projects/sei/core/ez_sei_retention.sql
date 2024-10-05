{{ 
    config(
        materialized="table",
        snowflake_warehouse="sei",
        database="sei",
        schema="core",
        alias="ez_cohort_retention",
    )
 }}

{{ get_cohort_retention("sei") }}