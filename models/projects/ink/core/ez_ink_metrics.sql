{{
    config(
        materialized="table",
        snowflake_warehouse="INK",
        database="ink",
        schema="core",
        alias="ez_metrics",
    )
}}

with 
    ink_dex_volumes as (
        select date, daily_volume as dex_volumes
        from {{ ref("fact_ink_daily_dex_volumes") }}
    )
select
    date
    , txns
    , daa as dau
    , fees_native
    , fees
    , ink_dex_volumes.dex_volumes
from {{ ref("fact_ink_fundamental_metrics") }}
left join ink_dex_volumes using (date)
where date < to_date(sysdate())
