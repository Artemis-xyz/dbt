{{
    config(
        materialized="table",
        snowflake_warehouse="STARKNET",
    )
}}

with supply_data as (
    select
        date(date) as date
        , community_supply
        , insiders_supply
        , gross_emissions_native
        , premine_unlocks_native
        , burns_native
        , net_supply_change_native
        , circulating_supply_native
    from {{ source('MANUAL_STATIC_TABLES', 'starknet_daily_supply_data') }}
)

, date_spine as (
    select
        date
    from PC_DBT_DB.PROD.dim_date_spine
    where date between 
        (select min(date) from supply_data)
        and 
        (select max(date) from supply_data)
)

, joined_data as (
    select
        ds.date
        , sd.community_supply
        , sd.insiders_supply
        , sd.gross_emissions_native
        , sd.premine_unlocks_native
        , sd.burns_native
        , sd.net_supply_change_native
        , sum(sd.circulating_supply_native) 
            over (order by ds.date) as circulating_supply_native
    from date_spine ds
    left join supply_data sd using (date)
)

, forward_filled_supply_data as (
    select
        date
        , coalesce(gross_emissions_native, 0) as gross_emissions_native
        , coalesce(premine_unlocks_native, 0) as premine_unlocks_native
        , coalesce(net_supply_change_native, 0) as net_supply_change_native
        , coalesce(burns_native, 0) as burns_native
        , last_value(circulating_supply_native ignore nulls) over (
            order by date
            rows between unbounded preceding and current row
        ) as circulating_supply_native
    from joined_data
)

select
    date
    , gross_emissions_native
    , premine_unlocks_native
    , net_supply_change_native
    , circulating_supply_native
    , burns_native
from forward_filled_supply_data