{{
    config(
        materialized="table",
        snowflake_warehouse="INK",
        database="ink",
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
from {{ ref("fact_ink_fundamental_metrics") }}
where date < to_date(sysdate())
