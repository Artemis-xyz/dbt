{{
    config(
        materialized = "table",
        snowflake_warehouse = "CHILIZ",
        database = "CHILIZ",
        schema = "core",
        alias = "ez_metrics"
    )
}}

with fees as (
    select
        date,
        fees_usd
    from {{ref("fact_chiliz_fees")}}
),
txns as (
    select
        date,
        txns
    from {{ref("fact_chiliz_txns")}}
)
, daus as (
    select
        date,
        dau
    from {{ref("fact_chiliz_dau")}}
    where dau < 170000 -- There is a DQ issue with the Chiliz dau data: 2 days with > 170k DAU while the rest of the data around those days is < 1k
)

select
    coalesce(fees.date, txns.date, daus.date) as date,
    dau,
    txns,
    fees_usd as fees
from fees
left join txns on fees.date = txns.date
left join daus on fees.date = daus.date 