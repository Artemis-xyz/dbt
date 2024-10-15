{{
    config(
        materialized="table",
        snowflake_warehouse="developer",
        database="developer",
        schema="core",
        alias="ez_github_subecosystem_weekly_commits",
    )
}}

select 
    date
    , ecosystem_name
    , val
from {{ source("STAGING", "core_weeklycommitssubecosystems") }} as commits
left join {{ source("STAGING", "core_ecosystems") }} as ecosystems on commits.ecosystem_id = ecosystems.id
