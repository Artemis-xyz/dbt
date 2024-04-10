{{ 
    config(
        materialized="table",
        snowflake_warehouse="optimism",
        database="optimism",
        schema="core",
        alias="ez_cohort_retention",
    )
 }}

{{ get_cohort_retention("optimism") }}