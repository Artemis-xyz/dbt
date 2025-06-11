{{
    config(
        materialized="table",
        snowflake_warehouse="CELESTIA"
    )
}}

with premine_unlocks as (
    select 
        date
        , premine_unlocks_native
    from {{ source("MANUAL_STATIC_TABLES", "celestia_premine_unlocks_data") }}
)

select 
    coalesce(premine_unlocks.date, celestia_mints.date) as date
    , coalesce(premine_unlocks_native, 0) as premine_unlocks_native
    , coalesce(mints, 0) as gross_emissions_native
    , coalesce(premine_unlocks_native, 0) + coalesce(mints, 0) as net_supply_change_native
    , sum(coalesce(net_supply_change_native, 0)) over (order by date) as circulating_supply_native
from premine_unlocks
left join {{ ref("fact_celestia_mints_silver") }} as celestia_mints using (date)
