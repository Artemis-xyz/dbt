{{ 
    config(
        materialized="table",
        snowflake_warehouse="ethereum",
        database="ethereum",
        schema="core",
        alias="ez_cohort_retention",
    )
 }}

{{ get_cohort_retention("ethereum") }}