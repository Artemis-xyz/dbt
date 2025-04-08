{{
    config(
        materialized="table",
        snowflake_warehouse="STORY",
        database="story",
        schema="core",
        alias="ez_metrics",
    )
}}

select
    date
    , txns
    , daa as dau
    , fees_native
    , fees
from {{ ref("fact_story_fundamental_metrics") }}
where date < to_date(sysdate())
