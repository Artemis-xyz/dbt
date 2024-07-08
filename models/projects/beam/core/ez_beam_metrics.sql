{{
    config(
        materialized="table",
        snowflake_warehouse="BEAM",
        database="beam",
        schema="core",
        alias="ez_metrics",
    )
}}

with
    fundamental_data as (
        select
            date, chain, dau, txns, fees_native
        from {{ ref("fact_beam_fundamental_metrics") }}
    ),
    price_data as ({{ get_coingecko_metrics("beam-2") }}),
    defillama_data as ({{ get_defillama_metrics("beam") }})
select
    fundamental_data.date,
    chain,
    dau,
    txns,
    fees_native,
    fees_native * price as fees,
    price,
    market_cap,
    fdmc,
    tvl
from fundamental_data
left join price_data on fundamental_data.date = price_data.date
left join defillama_data on fundamental_data.date = defillama_data.date
where fundamental_data.date < to_date(sysdate())
