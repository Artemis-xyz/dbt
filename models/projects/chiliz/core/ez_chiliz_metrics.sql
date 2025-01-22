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
, burns as (
    select
        date,
        burns_native,
        revenue
    from {{ref("fact_chiliz_burns")}}
)
, treasury as (
    select
        date,
        native_balance,
        native_balance_change,
        usd_balance,
        usd_balance_change
    from {{ref("fact_chiliz_treasury")}}
)
select
    coalesce(fees.date, txns.date, daus.date) as date,
    dau,
    txns,
    fees_usd as fees,
    revenue,
    burns_native as burns_native,
    treasury_usd as treasury_value,
    treasury_native_balance_change as treasury_value_native_change
from fees
left join txns using (date)
left join daus using (date)
left join burns using (date)
left join treasury using (date)