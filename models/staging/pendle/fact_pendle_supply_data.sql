{{
    config(
        materialized="table",
        snowflake_warehouse="PENDLE"
    )
}}

with emissions as (
    SELECT
        date,
        sum(token_incentives_native) as emissions_native
    FROM
        {{ ref('fact_pendle_token_incentives_by_chain')}}
    GROUP BY 1
)
, ve_pendle_locked as (
    SELECT
        date,
        sum(balance_native) as ve_pendle_balance
    FROM {{ ref('fact_pendle_vependle_balance') }}
    WHERE contract_address = '0x808507121b80c02388fad14726482e061b8da827'
    GROUP BY 1
)
, date_spine as (
    SELECT
        date
    FROM
        {{ ref('dim_date_spine')}}
    WHERE date between '2021-04-27' and to_date(sysdate())
)
SELECT
    ds.date,
    coalesce(advisors + liquidity_boostrapping + investors + ecosystem_fund + team, 0) as unlocks_native,
    sum(unlocks_native) over (order by date asc) as unlocks_native_cumulative,
    coalesce(emissions_native, 0) as emissions_native,
    sum(emissions_native) over (order by date asc) as emissions_native_cumulative,
    coalesce(ve_pendle_balance, 0) as pendle_locked,
    unlocks_native_cumulative + emissions_native_cumulative - pendle_locked as circulating_supply
FROM date_spine ds
LEFT JOIN {{ source("MANUAL_STATIC_TABLES", "pendle_unlocks_seed") }} using(date)
LEFT JOIN emissions using(date)
LEFT JOIN ve_pendle_locked using(date)