{{
    config(
        materialized = "table",
        snowflake_warehouse = "CHILIZ",
        database = "CHILIZ",
        schema = "CORE",
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
)

select
    coalesce(fees.date, txns.date, daus.date) as date,
    dau,
    txns,
    fees_usd as fees
from fees
left join txns on fees.date = txns.date
left join daus on fees.date = daus.date 