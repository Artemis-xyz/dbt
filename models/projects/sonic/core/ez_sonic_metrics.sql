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
    , defillama_data as ({{ get_defillama_metrics("sonic") }})
    , github_data as ({{ get_github_metrics("sonic") }})
select
    fundamentals.date
    , fundamentals.fees
    , fundamentals.txns
    , fundamentals.dau
    , sonic_dex_volumes.dex_volumes
    , price_data.price
    , defillama_data.tvl
    , github_data.commits
from fundamentals
left join sonic_dex_volumes on fundamentals.date = sonic_dex_volumes.date
left join price_data on fundamentals.date = price_data.date
left join defillama_data on fundamentals.date = defillama_data.date
left join github_data on fundamentals.date = github_data.date
where date < to_date(sysdate())
