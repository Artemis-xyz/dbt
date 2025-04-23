{{
    config(
        materialized="table",
        snowflake_warehouse="ORCA",
    )
}}


with treasury_balance as (
    SELECT
        date,
        sum(balance_native) as non_circulating
    FROM {{ ref("fact_orca_treasury_balance") }}
    where contract_address = 'orcaEKTdK7LKz57vaAYr9QeNsVEPfiu6QeMU1kektZE'
    GROUP BY date
)

, supply_data as (
    SELECT
        date,
        total
    FROM {{ source("MANUAL_STATIC_TABLES", "orca_daily_supply_data")}}
)

SELECT
    treasury_balance.date
    , 94250000 + 5250000 - non_circulating as premine_unlocks
    -- , total as premine_unlocks_2
    -- , sum(coalesce(premine_unlocks, 0) + coalesce(premine_unlocks_2, 0)) over (order by treasury_balance.date) as circulating
    , sum(coalesce(premine_unlocks, 0) ) over (order by treasury_balance.date) as circulating_2
FROM treasury_balance
    