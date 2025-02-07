--depends_on: {{ ref("fact_acala_rolling_active_addresses") }}
{{
    config(
        materialized="table",
        snowflake_warehouse="ACALA",
        database="acala",
        schema="core",
        alias="ez_metrics",
    )
}}

with
    fundamental_data as (
        select
            date, chain, daa, txns, fees_native, fees_usd as fees, fees_usd * .2 as revenue
        from {{ ref("fact_acala_fundamental_metrics") }}
    ),
    rolling_metrics as ({{ get_rolling_active_address_metrics("acala") }})
select
    fundamental_data.date,
    fundamental_data.chain,
    daa as dau,
    txns,
    fees_native,
    fees,
    fees / txns as avg_txn_fee,
    revenue,
    wau,
    mau
from fundamental_data
left join rolling_metrics on fundamental_data.date = rolling_metrics.date
where fundamental_data.date < to_date(sysdate())
