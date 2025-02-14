{{
    config(
        materialized="table",
        snowflake_warehouse="MOONBEAM",
        database="moonbeam",
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
        from {{ ref("fact_moonbeam_fundamental_metrics") }}
    )
select
    date
    , txns
    , daa as dau
    , coalesce(fees_native, 0) as fees_native
    , coalesce(fees_usd, 0) as fees
from fundamental_data
where fundamental_data.date < to_date(sysdate())
