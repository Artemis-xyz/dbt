{{
    config(
        materialized="table",
        snowflake_warehouse="SONEIUM",
        database="soneium",
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
from {{ ref("fact_soneium_fundamental_metrics") }}
where date < to_date(sysdate())
