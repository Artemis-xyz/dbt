{{
    config(
        materialized="table",
        snowflake_warehouse="ONDO",
    )
}}

with date_spine as (
    SELECT
        date
    FROM {{ ref("dim_date_spine") }}
    where date between '2024-01-16' and to_date(sysdate())
)

SELECT
    date,
    coalesce(premine_unlocks, 0) as premine_unlocks_native,
    coalesce(premine_unlocks, 0) as net_supply_change_native,
    sum(net_supply_change_native) over (order by date) as circulating_supply_native
from date_spine
left join {{ source("MANUAL_STATIC_TABLE", "ondo_daily_supply_data") }} using (date)
