{{
    config(
        materialized = "table",
        snowflake_warehouse = "METIS",
    )
}}

with date_spine as (
    SELECT
        date
    FROM {{ ref('dim_date_spine') }}
    WHERE date between '2021-05-12' and to_date(sysdate())
)

select
    ds.date
    , net_supply_change as premine_unlocks_native
    , net_supply_change as net_supply_change_native
    , circulating_supply as circulating_supply_native
from date_spine ds
left join {{ source('MANUAL_STATIC_TABLES', 'metis_daily_supply_data') }} using (date)
