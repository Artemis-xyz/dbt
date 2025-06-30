{{
    config(
        materialized="table",
        snowflake_warehouse="developer",
        database="developer",
        schema="raw",
        alias="ez_github_repository_subecosystems",
    )
}}

select 
    ecosystem_name,
    subecosystem_name
from {{ source("STAGING", "core_subecosystems") }}
left join {{ source("STAGING", "core_ecosystems") }} on core_subecosystems.ecosystem_id = core_ecosystems.id
