{{
    config(
        materialized="table",
        snowflake_warehouse="RESERVE",
    )
}}

with supply_data as (
    select
        date(date) as date
        , team_cumulative
        , investors_cumulative
        , treasury_cumulative
        , private_sale_investor_cumulative
        , public_cumulative
        , premine_unlocks_supply_native
        , gross_emissions_native
        , burns_native
        , net_supply_change_native
        , circulating_supply_native
    from {{ source('MANUAL_STATIC_TABLES', 'reserve_daily_supply_data') }}
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

, combined as (
    select
        ds.date
        , sd.team_cumulative
        , sd.investors_cumulative
        , sd.treasury_cumulative
        , sd.private_sale_investor_cumulative
        , sd.public_cumulative
        , sd.premine_unlocks_supply_native
        , sd.gross_emissions_native
        , sd.burns_native
        , sd.net_supply_change_native
        , sd.circulating_supply_native
    from date_spine ds
    left join supply_data sd on ds.date = sd.date
)

, forward_filled_supply as (
    select
        date
        , last_value(team_cumulative ignore nulls) over (order by date rows between unbounded preceding and current row) as team_cumulative
        , last_value(investors_cumulative ignore nulls) over (order by date rows between unbounded preceding and current row) as investors_cumulative
        , last_value(treasury_cumulative ignore nulls) over (order by date rows between unbounded preceding and current row) as treasury_cumulative
        , last_value(private_sale_investor_cumulative ignore nulls) over (order by date rows between unbounded preceding and current row) as private_sale_investor_cumulative
        , last_value(public_cumulative ignore nulls) over (order by date rows between unbounded preceding and current row) as public_cumulative
        , coalesce(premine_unlocks_supply_native, 0) as premine_unlocks_supply_native
        , last_value(gross_emissions_native ignore nulls) over (order by date rows between unbounded preceding and current row) as gross_emissions_native
        , coalesce(burns_native, 0) as burns_native
        , coalesce(net_supply_change_native, 0) as net_supply_change_native
        , last_value(circulating_supply_native ignore nulls) over (order by date rows between unbounded preceding and current row) as circulating_supply_native
    from combined
)
select 
    date
    , team_cumulative
    , investors_cumulative
    , treasury_cumulative
    , private_sale_investor_cumulative
    , public_cumulative
    , premine_unlocks_supply_native as premine_unlocks_native
    , gross_emissions_native
    , burns_native
    , net_supply_change_native
    , circulating_supply_native
from forward_filled_supply