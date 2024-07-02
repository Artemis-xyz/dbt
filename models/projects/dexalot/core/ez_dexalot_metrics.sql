{{
    config(
        materialized="table",
        snowflake_warehouse="DEXALOT",
        database="DEXALOT",
        schema="core",
        alias="ez_metrics",
    )
}}

with
    fundamental_data as (
        select
            date, chain, dau, txns, fees_native
        from {{ ref("fact_dexalot_fundamental_metrics") }}
    ),
    price_data as ({{ get_coingecko_metrics("dexalot") }})
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
