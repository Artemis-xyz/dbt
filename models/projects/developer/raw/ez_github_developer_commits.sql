{{
    config(
        materialized="table",
        snowflake_warehouse="developer",
        database="developer",
        schema="raw",
        alias="ez_github_developer_commits",
    )
}}

select
    id 
    , repo_full_name 
    , start_of_week
    , author_id
    , num_commits 
    , num_additions
    , num_deletions 
from 
    {{source("STAGING", "core_weeklycommithistory")}}
where 
    start_of_week <= {{ latest_developer_data_date() }}
