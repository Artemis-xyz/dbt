{{
    config(
        materialized="table",
        snowflake_warehouse="developer",
        database="developer",
        schema="core",
        alias="ez_github_ecosystem_weekly_devs_no_forks",
    )
}}

select 
    date
    , ecosystem_name
    , val
from {{ source("STAGING", "core_weeklydevscoreecosystemwithoutforks") }} as devs
left join {{ source("STAGING", "core_ecosystems") }} as ecosystems on devs.ecosystem_id = ecosystems.id
where 
    date <= {{ latest_developer_data_date() }}