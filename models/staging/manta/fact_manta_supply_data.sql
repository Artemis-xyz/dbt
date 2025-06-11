{{
    config(
        materialized="table",
        snowflake_warehouse="MANTA",
    )
}}

with date_spine as (
    SELECT date FROM {{ ref('dim_date_spine') }}
    WHERE date between '2024-01-18' and to_date(sysdate())
)

SELECT
    ds.date
    , 1e9 * 0.02 / 365 as gross_emissions_native -- Annual inflation of 2% of 1bn
    , coalesce(total_monthly, 0) as premine_unlocks_native
    , gross_emissions_native + premine_unlocks_native as net_supply_change_native
    , sum(net_supply_change_native) over (order by ds.date) as circulating_supply_native
FROM
    date_spine ds
LEFT JOIN
    {{ source("MANUAL_STATIC_TABLES", "manta_daily_supply_data")}} using(date)