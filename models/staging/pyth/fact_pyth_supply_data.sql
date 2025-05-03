{{
    config(
        materialized='table',
        unique_key='date',
        snowflake_warehouse='PYTH',
    )
}}


with date_spine as (
    select
        ds.date
    from {{ ref('dim_date_spine') }} ds
    where ds.date between (select min(date) from {{ ref('fact_pyth_unlock_data') }}) and to_date(sysdate())
)
, unlock_data as (
    select
        date,
        premine_unlocks_native
    from {{ ref('fact_pyth_unlock_data') }}
)
SELECT
    ds.date,
    coalesce(ud.premine_unlocks_native, 0) as premine_unlocks_native,
    coalesce(ud.premine_unlocks_native, 0) as net_supply_change_native,
    sum(coalesce(ud.premine_unlocks_native, 0)) over (order by ds.date) as circulating_supply_native
from date_spine ds
left join unlock_data ud using (date)
