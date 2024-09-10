--depends_on {{ ref("fact_zksync_rolling_active_addresses") }}
{{
    config(
        materialized="table",
        snowflake_warehouse="ZKSYNC",
        database="zksync",
        schema="core",
        alias="ez_metrics",
    )
}}

with
    fundamental_data as (
        select
            date,
            chain,
            daa as dau,
            txns,
            gas,
            gas_usd,
        from {{ ref("fact_zksync_daa_txns_gas_gas_usd") }}
    ),
    rolling_metrics as ({{ get_rolling_active_address_metrics("zksync") }}),
    revenue_data as (
        select date, revenue, revenue_native, l1_data_cost, l1_data_cost_native
        from {{ ref("fact_zksync_revenue") }}
    )
select
    fundamental_data.date,
    fundamental_data.chain,
    dau,
    mau,
    wau,
    txns,
    gas as fees_native,
    gas_usd as fees,
    revenue,
    revenue_native,
    l1_data_cost,
    l1_data_cost_native
from fundamental_data
left join rolling_metrics on fundamental_data.date = rolling_metrics.date
left join revenue_data on fundamental_data.date = revenue_data.date
where fundamental_data.date < to_date(sysdate())
