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
        select date, hnt_burned, revenue, chain, protocol
        from {{ ref("fact_helium_revenue_silver") }}
    ),
    fees_data as(
        select date, fees, chain, protocol
        from {{ ref("fact_helium_fees_silver") }}
    ),
    new_mobile_subscribers_data as (
        select date, new_subscribers, 'solana' as chain, 'helium' as protocol
        from {{ ref("fact_helium_new_mobile_subscribers") }}
    ),
    new_hotspot_onboards_data as (
        select date, device_onboards, 'solana' as chain, 'helium' as protocol
        from {{ ref("fact_helium_new_hotspot_onboards") }}
    ),
    price_data as ({{ get_coingecko_metrics("helium") }})
select
    revenue_data.date,
    revenue_data.chain,
    revenue_data.protocol,
    revenue_data.revenue,
    revenue_data.hnt_burned as burns_native,
    new_mobile_subscribers_data.new_subscribers,
    new_hotspot_onboards_data.device_onboards,
    fees_data.fees,
    price_data.price,
    price_data.market_cap,
    price_data.fdmc
from revenue_data
left join price_data using (date)
left join fees_data using (date)
left join new_mobile_subscribers_data using (date)
left join new_hotspot_onboards_data using (date)
where revenue_data.date < to_date(sysdate())
order by 1 desc