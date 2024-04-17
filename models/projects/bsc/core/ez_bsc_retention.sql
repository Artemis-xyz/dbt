{{ 
    config(
        materialized="table",
        snowflake_warehouse="bsc",
        database="bsc",
        schema="core",
        alias="ez_cohort_retention",
    )
 }}

{{ get_cohort_retention("bsc") }}