{{
    config(
        materialized="table",
        snowflake_warehouse="developer",
        database="developer",
        schema="core",
        alias="ez_github_ecosystem_weekly_commits",
    )
}}

select 
    date
    , ecosystem_name
    , val
from {{ source("STAGING", "core_weeklycommitscoreecosystem") }} as commits
left join {{ source("STAGING", "core_ecosystems") }} as ecosystems on commits.ecosystem_id = ecosystems.id
where 
    start_of_week <= {{ latest_developer_data_date() }}

