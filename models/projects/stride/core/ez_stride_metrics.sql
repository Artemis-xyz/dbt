--depends_on: {{ ref("fact_stride_rolling_active_addresses") }}

{{
    config(
        materialized="table",
        snowflake_warehouse="STRIDE",
        database="stride",
        schema="core",
        alias="ez_metrics",
    )
}}

with
    fundamental_metrics as (
        select
            date, chain, daa, gas_usd, txns
        from {{ ref("fact_stride_daa_gas_usd_txns") }}
    ),
    rolling_metrics as ({{ get_rolling_active_address_metrics("stride") }})
select
    fundamental_metrics.date,
    fundamental_metrics.chain,
    daa as dau,
    gas_usd as fees,
    txns,
    wau,
    mau,
from fundamental_metrics
left join rolling_metrics on fundamental_metrics.date = rolling_metrics.date
where fundamental_metrics.date < to_date(sysdate())