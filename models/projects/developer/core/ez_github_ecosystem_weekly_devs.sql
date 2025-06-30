{{
    config(
        materialized="table",
        snowflake_warehouse="developer",
        database="developer",
        schema="core",
        alias="ez_github_ecosystem_weekly_devs",
    )
}}

select 
    date
    , ecosystem_name
    , val
from {{ source("STAGING", "core_weeklydevscoreecosystem") }} as commits
left join {{ source("STAGING", "core_ecosystems") }} as ecosystems on commits.ecosystem_id = ecosystems.id
where 
    date <= {{ latest_developer_data_date() }}
