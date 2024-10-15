{{
    config(
        materialized="table",
        snowflake_warehouse="developer",
        database="developer",
        schema="core",
        alias="ez_github_subecosystem_weekly_devs",
    )
}}

select 
    date
    , ecosystem_name
    , val
from {{ source("staging", "core_weeklydevssubecosystems") }} as commits
left join {{ source("staging", "core_ecosystems") }} as ecosystems on commits.ecosystem_id = ecosystems.id
