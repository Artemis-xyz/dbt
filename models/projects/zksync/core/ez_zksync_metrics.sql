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
            median_gas_usd as median_txn_fee
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
    ),
    zksync_dex_volumes as (
        select date, daily_volume as dex_volumes
        from {{ ref("fact_zksync_daily_dex_volumes") }}
    )
    , supply_data as (
        select
            date
            , gross_emissions_native
            , premine_unlocks_native
            , burns_native
            , net_supply_change_native
            , circulating_supply_native
        from {{ ref("fact_zksync_supply_data") }}
    )
    , price_data as (
        {{ get_coingecko_metrics('zksync') }}
    )
select
    f.date
    , dune_dex_volumes_zksync.dex_volumes
    -- Old metrics needed for compatibility
    , f.chain
    , dau
    , mau
    , wau
    , txns
    , gas as fees_native
    , gas_usd as fees
    , fees / txns as avg_txn_fee
    , median_txn_fee
    , revenue
    , revenue_native
    , l1_data_cost
    , l1_data_cost_native
    , bridge_daa_metrics.bridge_daa

    -- Standardized Metrics
    -- Market Data
    , price
    , market_cap
    , fdmc
    , token_volume
    -- chain metrics
    , dau as chain_dau
    , wau as chain_wau
    , mau as chain_mau
    , txns as chain_txns
    , avg_txn_fee as chain_avg_txn_fee
    , median_txn_fee as chain_median_txn_fee
    , dune_dex_volumes_zksync.dex_volumes as chain_spot_volume
    -- Cash Flow Metrics
    , gas_usd as ecosystem_revenue
    , gas as ecosystem_revenue_native
    , l1_data_cost as l1_cash_flow
    , l1_data_cost_native as l1_cash_flow_native
    , revenue as foundation_cash_flow
    , revenue_native as foundation_cash_flow_native
    -- Bridge Metrics
    , bridge_volume_metrics.bridge_volume as bridge_volume
    , bridge_daa_metrics.bridge_daa as bridge_dau
    , token_turnover_circulating
    , token_turnover_fdv
    -- Supply Data
    , gross_emissions_native
    , premine_unlocks_native
    , burns_native
    , net_supply_change_native
    , circulating_supply_native
from fundamental_data f
left join rolling_metrics on f.date = rolling_metrics.date
left join revenue_data on f.date = revenue_data.date
left join bridge_volume_metrics on f.date = bridge_volume_metrics.date
left join bridge_daa_metrics on f.date = bridge_daa_metrics.date
left join zksync_dex_volumes as dune_dex_volumes_zksync on f.date = dune_dex_volumes_zksync.date
left join price_data on f.date = price_data.date
left join supply_data on f.date = supply_data.date
where f.date < to_date(sysdate())
