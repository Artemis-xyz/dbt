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
    fundamentals as (
        SELECT
            date,
            fees,
            txns,
            dau
        FROM {{ ref("fact_sonic_fundamental_metrics") }}
    ),
    price_data as ({{ get_coingecko_metrics("sonic") }}),
    defillama_data as ({{ get_defillama_metrics("sonic") }}),
    github_data as ({{ get_github_metrics("sonic") }})
select
    fundamentals.date,
    fundamentals.fees,
    fundamentals.txns,
    fundamentals.dau
from fundamentals