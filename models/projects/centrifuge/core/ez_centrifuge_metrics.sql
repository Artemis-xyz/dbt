{{
    config(
        materialized="table",
        snowflake_warehouse="CENTRIFUGE",
        database="centrifuge",
        schema="core",
        alias="ez_metrics",
    )
}}
with
    fundamental_data as (
        select
            date, 
            txns,
            daa, 
            fees_native, 
            fees_usd
        from {{ ref("fact_centrifuge_fundamental_metrics") }}
    )
select
    date
    , txns
    , daa
    , coalesce(fees_native, 0) as fees_native
    , coalesce(fees_usd, 0) as fees_usd
from fundamental_data
where fundamental_data.date < to_date(sysdate())
