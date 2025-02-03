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
    date_spine as (
        select date
        from {{ ref("dim_date_spine") }}
        where date between '2020-04-17' and to_date(sysdate())
    ),
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
    mints_data as (
        select date, mints_native, 'solana' as chain, 'helium' as protocol
        from {{ ref("fact_helium_mints") }}
    ),
    price_data as ({{ get_coingecko_metrics("helium") }})
select
    date_spine.date,
    revenue_data.chain,
    revenue_data.protocol,
    revenue_data.revenue,
    revenue_data.hnt_burned as burns_native,
    mints_data.mints_native,
    new_mobile_subscribers_data.new_subscribers,
    new_hotspot_onboards_data.device_onboards,
    fees_data.fees,
    price_data.price,
    price_data.market_cap,
    price_data.fdmc
from date_spine
left join revenue_data using (date)
left join price_data using (date)
left join fees_data using (date)
left join new_mobile_subscribers_data using (date)
left join new_hotspot_onboards_data using (date)
left join mints_data using (date)
where revenue_data.date < to_date(sysdate())
order by 1 desc