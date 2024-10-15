{{
    config(
        materialized="table",
        snowflake_warehouse="developer",
        database="developer",
        schema="core",
        alias="ez_github_subecosystem_weekly_devs_no_forks",
    )
}}

select 
    date
    , ecosystem_name
    , val
from {{ source("staging", "core_weeklydevscoreecosystemwithoutforks") }} as devs
left join {{ source("staging", "core_ecosystems") }} as ecosystems on devs.ecosystem_id = ecosystems.id
