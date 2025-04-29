{{ config(materialized="table") }}

with uploaded_data as (
    select date, premine_unlocks_native, burns_native from {{source('MANUAL_STATIC_TABLES', 'flare_daily_supply_data')}}
    where date > '2021-04-21'
)
, with_gross_emissions as (
    select
        date,
        case when date between '2023-01-09' and '2024-01-08' then 27397260.27
        when date between '2024-01-09' and '2025-01-08' then 19178082.19
        when date > '2025-01-09' then 13698630.14
        else 0
        end as gross_emissions_native,
        premine_unlocks_native,
        burns_native,
        gross_emissions_native + premine_unlocks_native - burns_native as net_supply_change_native,
        sum(net_supply_change_native) over (order by date) as circulating_supply
    from uploaded_data
)
select * from with_gross_emissions