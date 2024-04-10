{{ 
    config(
        materialized="table",
        snowflake_warehouse="arbitrum",
        database="arbitrum",
        schema="core",
        alias="ez_cohort_retention",
    )
 }}

{{ get_cohort_retention("arbitrum") }}