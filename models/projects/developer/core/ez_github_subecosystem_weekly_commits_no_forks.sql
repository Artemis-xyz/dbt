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
from {{ source("STAGING", "core_weeklycommitssubecosystemswithoutforks") }} as commits
left join {{ source("STAGING", "core_ecosystems") }} as ecosystems on commits.ecosystem_id = ecosystems.id
where 
    date <= {{ latest_developer_data_date() }}
