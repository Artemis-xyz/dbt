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
    rolling_metrics as ({{ get_rolling_active_address_metrics("zksync") }})
select
    fundamental_data.date,
    fundamental_data.chain,
    dau,
    mau,
    wau
    txns,
    gas,
    gas_usd
from fundamental_data
left join rolling_metrics on fundamental_data.date = rolling_metrics.date
where fundamental_data.date < to_date(sysdate())
