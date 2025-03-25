{{
    config(
        materialized="table",
        snowflake_warehouse="COINDESK_20",
        database="COINDESK_20",
        schema="core",
        alias="ez_metrics",
    )
}}

select
    date
    , price
from {{ ref("fact_coindesk20_price") }}
