{{
    config(
        materialized="table",
        snowflake_warehouse="SUI",
    )
}}

with supply_data as (
    select
        date(date) as date
        , net_supply_change_native
        , circulating_supply_native
    from {{ source('MANUAL_STATIC_TABLES', 'sui_daily_supply_data') }}
)

, date_spine as (
    select
        date
    from {{ ref("dim_date_spine") }}
    where date between 
        (select min(date) from supply_data)
        and 
        (select max(date) from supply_data)
)

, joined as (
    select
        ds.date
        , sd.net_supply_change_native
        , sd.circulating_supply_native
    from date_spine ds
    left join supply_data sd on ds.date = sd.date
)

, forward_filled as (
    select
        date
        , coalesce(net_supply_change_native, 0) as net_supply_change_native
        , last_value(circulating_supply_native ignore nulls) over (
            order by date
            rows between unbounded preceding and current row
        ) as circulating_supply_native
    from joined
)

select 
    date
    , net_supply_change_native as premine_unlocks_native
    , net_supply_change_native
    , circulating_supply_native
from forward_filled
