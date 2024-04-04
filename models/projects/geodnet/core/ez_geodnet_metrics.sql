{{
    config(
        materialized="table",
        snowflake_warehouse="GEODNET",
        database="geodnet",
        schema="core",
        alias="ez_metrics",
    )
}}

with
    revenue_data as (
        select date, fees, revenue, chain, protocol
        from {{ ref("fact_geodnet_fees_revenue") }}
    ),
    price_data as ({{ get_coingecko_metrics("geodnet") }})
select
    revenue_data.date,
    revenue_data.chain,
    revenue_data.protocol,
    fees,
    revenue,
    price,
    market_cap,
    fdmc
from revenue_data
left join price_data on revenue_data.date = price_data.date
where revenue_data.date < to_date(sysdate())
