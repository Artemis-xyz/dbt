{{
    config(
        materialized = "table",
        snowflake_warehouse = "FLARE",
        database = "FLARE",
        schema = "core",
        alias = "ez_metrics"
    )
}}

with fees as (
    select
        date,
        fees_usd
    from {{ref("fact_flare_fees")}}
),
txns as (
    select
        date,
        txns
    from {{ref("fact_flare_txns")}}
)
, daus as (
    select
        date,
        dau
    from {{ref("fact_flare_dau")}}
)

select
    coalesce(fees.date, txns.date, daus.date) as date,
    dau,
    txns,
    fees_usd as fees
from fees
left join txns on fees.date = txns.date
left join daus on fees.date = daus.date 