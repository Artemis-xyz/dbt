{{
    config(
        materialized="table",
        snowflake_warehouse="developer",
        database="developer",
        schema="raw",
        alias="ez_github_repository_subecosystems",
    )
}}

select 
    ecosystem,
    subecosystem_name
from {{ source("STAGING", "core_subecosystems") }}
