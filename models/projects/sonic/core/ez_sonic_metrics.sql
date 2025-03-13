{{
    config(
        materialized="table",
        snowflake_warehouse="SONIC",
        database="sonic",
        schema="core",
        alias="ez_metrics",
    )
}}

with 
    sonic_dex_volumes as (
        select date, daily_volume as dex_volumes
        from {{ ref("fact_sonic_daily_dex_volumes") }}
    )
    , fundamentals as (
        SELECT
            date,
            fees,
            txns,
            dau
        FROM {{ ref("fact_sonic_fundamental_metrics") }}
    )
    , price_data as ({{ get_coingecko_metrics("sonic") }})
select
    fundamentals.date
    , fundamentals.fees
    , fundamentals.txns
    , fundamentals.dau
    , sonic_dex_volumes.dex_volumes
    , price_data.price
from fundamentals
left join sonic_dex_volumes on fundamentals.date = sonic_dex_volumes.date
left join price_data on fundamentals.date = price_data.date
where fundamentals.date < to_date(sysdate())
