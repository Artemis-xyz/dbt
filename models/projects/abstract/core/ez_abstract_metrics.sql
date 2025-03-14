{{
    config(
        materialized="table",
        snowflake_warehouse="ABSTRACT",
        database="abstract",
        schema="core",
        alias="ez_metrics",
    )
}}

select
    f.date
    , txns
    , daa as dau
    , fees_native
    , fees
    , cost
    , cost_native
    , revenue
    , revenue_native
from {{ ref("fact_abstract_fundamental_metrics") }} as f
where f.date  < to_date(sysdate())
