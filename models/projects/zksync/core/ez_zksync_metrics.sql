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
    ),
    bridge_volume_metrics as (
        select date, bridge_volume
        from {{ ref("fact_zksync_era_bridge_bridge_volume") }}
        where chain is null
    ),
    bridge_daa_metrics as (
        select date, bridge_daa
        from {{ ref("fact_zksync_era_bridge_bridge_daa") }}
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
    fees / txns as avg_txn_fee,
    revenue,
    revenue_native,
    l1_data_cost,
    l1_data_cost_native,
    bridge_volume.bridge_volume,
    bridge_daa.bridge_daa
from fundamental_data
left join rolling_metrics on fundamental_data.date = rolling_metrics.date
left join revenue_data on fundamental_data.date = revenue_data.date
left join bridge_volume_metrics on fundamental_data.date = bridge_volume.date
left join bridge_daa_metrics on fundamental_data.date = bridge_daa.date
where fundamental_data.date < to_date(sysdate())
