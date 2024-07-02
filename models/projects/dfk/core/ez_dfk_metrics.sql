{{
    config(
        materialized="table",
        snowflake_warehouse="DFK",
        database="DFK",
        schema="core",
        alias="ez_metrics",
    )
}}

with
    fundamental_data as (
        select
            date, chain, dau, txns, fees_native
        from {{ ref("fact_dfk_fundamental_metrics") }}
    ),
    price_data as ({{ get_coingecko_metrics("defi-kingdoms") }})
select
    fundamental_data.date,
    chain,
    dau,
    txns,
    fees_native,
    fees_native * price as fees,
    price,
    market_cap,
    fdmc
from fundamental_data
left join price_data on fundamental_data.date = price_data.date
where fundamental_data.date < to_date(sysdate())
