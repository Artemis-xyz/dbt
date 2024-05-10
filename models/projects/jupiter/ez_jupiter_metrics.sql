{{
    config(
        materialized="table",
        snowflake_warehouse="JUPITER",
        database="jupiter",
        schema="core",
        alias="ez_metrics",
    )
}}

with
    fees_data as (
        select date, fees, 'solana' as chain, 'jupiter' as protocol
        from {{ ref("fact_jupiter_fees_silver") }}
    ),
    price_data as ({{ get_coingecko_metrics("jupiter-exchange-solana") }})
select
    fees_data.date,
    fees_data.chain,
    fees_data.protocol,
    fees,
    price,
    market_cap,
    fdmc
from fees_data
left join price_data on fees_data.date = price_data.date
where fees_data.date < to_date(sysdate())
