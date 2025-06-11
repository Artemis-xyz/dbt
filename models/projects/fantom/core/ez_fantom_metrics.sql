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
    contract_data as ({{ get_contract_metrics("fantom") }}),
    rolling_metrics as ({{ get_rolling_active_address_metrics("fantom") }}),
    fantom_dex_volumes as (
        select date, daily_volume as dex_volumes, daily_volume_adjusted as adjusted_dex_volumes
        from {{ ref("fact_fantom_daily_dex_volumes") }}
    ),
    price_data as ({{ get_coingecko_metrics("fantom") }})
select
    d.date
    , d.chain
    , daa as dau
    , txns
    , gas as fees_native
    , fees
    , fees / txns as avg_txn_fee
    , revenue
    , wau
    , mau
    , dex_volumes
    -- Standardized Metrics
    -- Market Data
    , price
    , market_cap
    , fdmc
    , token_volume
    -- Chain Metrics
    , txns as chain_txns
    , dau as chain_dau
    , wau as chain_wau
    , mau as chain_mau
    , avg_txn_fee as chain_avg_txn_fee
    , dex_volumes as chain_spot_volume
    , adjusted_dex_volumes as chain_spot_volume_adjusted
    -- Cash Flow Metrics
    , fees as chain_fees
    , fees as ecosystem_revenue
    , fees_native as ecosystem_revenue_native
    , revenue as foundation_fee_allocation
    -- Developer Metrics
    , weekly_contracts_deployed
    , weekly_contract_deployers
    , token_turnover_circulating
    , token_turnover_fdv
from daa_gold d
left join price_data using (d.date)
left join contract_data using (d.date)
left join txns_gold using (d.date)
left join gas_gold using (d.date)
left join rolling_metrics using (d.date)
left join fantom_dex_volumes using (d.date)
where d.date < to_date(sysdate())
