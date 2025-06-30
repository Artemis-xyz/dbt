{{
    config(
        materialized="table",
        snowflake_warehouse="developer",
        database="developer",
        schema="raw",
        alias="ez_github_repository_ecosystems",
    )
}}

select 
    repo_full_name
    , last_updated
    , version
    , update_status
    , forked_from
    , ecosystem_name
    , symbol 
from {{ source("STAGING", "core_ecosystemrepositories") }} as er
left join {{ source("STAGING", "core_ecosystems") }} as ecosystems  on er.ecosystem_id = ecosystems.id
where
    last_updated <= {{ latest_developer_data_date() }}
