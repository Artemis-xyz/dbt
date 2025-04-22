{{ config(materialized="table") }}

select
    date,
    0 as emissions_native,
    premine_unlocks_native,
    0 as burns_native,
    emissions_native + premine_unlocks_native - burns_native as net_supply_change_native,
    sum(emissions_native + premine_unlocks_native - burns_native) over (order by date) as circulating_supply_native
from {{source('MANUAL_STATIC_TABLES', 'hivemapper_daily_premine_unlocks')}}
where date > '2021-04-19'