--depends_on: {{ ref("fact_fantom_rolling_active_addresses") }}
{{
    config(
        materialized="table",
        snowflake_warehouse="FANTOM",
        database="fantom",
        schema="core",
        alias="ez_metrics",
    )
}}

with
    daa_gold as (
        select
            date, chain, daa
        from {{ ref("fact_fantom_daa") }}
    ),
    txns_gold as (
        select
            date, chain, txns
        from {{ ref("fact_fantom_txns") }}
    ),
    gas_gold as (
        select
            date, chain, gas, gas_usd, fees, revenue
        from {{ ref("fact_fantom_gas_gas_usd_fees_revenue") }}
    ),
    rolling_metrics as ({{ get_rolling_active_address_metrics("fantom") }})
select
    daa_gold.date,
    daa_gold.chain,
    daa as dau,
    txns,
    gas as fees_native,
    fees,
    revenue,
    wau,
    mau,
from daa_gold
left join txns_gold on daa_gold.date = txns_gold.date
left join gas_gold on daa_gold.date = gas_gold.date
left join rolling_metrics on daa_gold.date = rolling_metrics.date
where daa_gold.date < to_date(sysdate())