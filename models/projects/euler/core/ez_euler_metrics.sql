{{
    config(
        materialized="table",
        snowflake_warehouse="EULER",
        database="euler",
        schema="core",
        alias="ez_metrics",
    )
}}

with lending_metrics as (
    select
        date
        , sum(supplied_amount_cumulative) as lending_supply
        , sum(borrow_amount_cumulative) as lending_borrow
        , sum(supplied_amount_cumulative - borrow_amount_cumulative) as tvl
    from {{ ref("fact_euler_borrow_and_lending_metrics_by_chain") }}
    group by 1
)
, market_metrics as (
    {{get_coingecko_metrics("euler")}}
)
, date_spine as (
    SELECT
        date
    from {{ ref("dim_date_spine") }}
    where date between (select min(date) from lending_metrics) and to_date(sysdate())
)
select
    ds.date
    , coalesce(lending_supply, 0) as lending_supply
    , coalesce(lending_borrow, 0) as lending_borrow
    , coalesce(tvl, 0) as tvl
    , coalesce(market_metrics.price, 0) as price
from date_spine ds
left join lending_metrics using (date)
left join market_metrics using (date)