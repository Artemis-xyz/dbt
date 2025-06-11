{{
    config(
        materialized="table",
        snowflake_warehouse="STARGATE",
    )
}}

with supply_data as (
    select
        date
        , team_supply
        , investors_supply
        , protocol_launch_supply
        , bonding_curve_supply
        , initial_emissions_program_supply
        , bonding_curve_dexs
        , community_supply
        , gross_emissions_native
        , net_change_supply_native as net_supply_change_native
        , burns_native
        , circulating_supply_native
        , premine_unlocks_native
    from {{ source('MANUAL_STATIC_TABLES', 'stargate_daily_supply_data') }}
)

, date_spine as (
    select
        date
    from {{ ref('dim_date_spine') }}
    where date between (select min(date) from supply_data) and to_date(sysdate())
)

, joined_data as (
    select
        ds.date,
        sd.gross_emissions_native,
        sd.premine_unlocks_native,
        sd.net_supply_change_native,
        sd.burns_native,
        sd.circulating_supply_native
    from date_spine ds
    left join supply_data sd using (date)
)

, forward_filled_supply_data as (
    select
        date,
        coalesce(gross_emissions_native, 0) as gross_emissions_native,
        coalesce(premine_unlocks_native, 0) as premine_unlocks_native,
        coalesce(net_supply_change_native, 0) as net_supply_change_native,
        coalesce(burns_native, 0) as burns_native,
        last_value(circulating_supply_native ignore nulls) over (
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