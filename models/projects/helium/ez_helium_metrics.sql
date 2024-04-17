{{
    config(
        materialized="table",
        snowflake_warehouse="HELIUM",
        database="helium",
        schema="core",
        alias="ez_metrics",
    )
}}

with
    revenue_data as (
        select date, revenue, chain, protocol
        from {{ ref("fact_helium_revenue_silver") }}
    ),
    fees_data as(
        select date, fees, chain, protocol
        from {{ ref("fact_helium_fees_silver") }}
    ),
    price_data as ({{ get_coingecko_metrics("helium") }})
select
    revenue_data.date,
    revenue_data.chain,
    revenue_data.protocol,
    revenue_data.revenue,
    fees_data.fees,
    price_data.price,
    price_data.market_cap,
    price_data.fdmc
from revenue_data
left join price_data on revenue_data.date = price_data.date
left join fees_data on fees_data.date = revenue_data.date
where revenue_data.date < to_date(sysdate())
order by 1 desc
