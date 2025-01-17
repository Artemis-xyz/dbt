{{ 
    config(
        materialized="table",
        snowflake_warehouse="mantle",
        database="mantle",
        schema="core",
        alias="ez_cohort_retention",
    )
 }}

{{ get_cohort_retention("mantle") }}