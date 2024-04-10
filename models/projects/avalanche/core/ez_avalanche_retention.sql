{{ 
    config(
        materialized="table",
        snowflake_warehouse="avalanche",
        database="avalanche",
        schema="core",
        alias="ez_cohort_retention",
    )
 }}

{{ get_cohort_retention("avalanche") }}