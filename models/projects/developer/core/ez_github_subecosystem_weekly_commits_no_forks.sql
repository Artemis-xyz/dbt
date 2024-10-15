{{
    config(
        materialized="table",
        snowflake_warehouse="developer",
        database="developer",
        schema="core",
        alias="ez_github_subecosystem_weekly_commits_no_forks",
    )
}}

select 
    date
    , ecosystem_name
    , val
from {{ source("staging", "core_weeklycommitssubecosystemswithoutforks") }} as commits
left join {{ source("staging", "core_ecosystems") }} as ecosystems on commits.ecosystem_id = ecosystems.id
