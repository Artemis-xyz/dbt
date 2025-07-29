{{
    config(
        materialized="incremental",
        snowflake_warehouse="APTOS",
        database="aptos",
        schema="core",
        alias="ez_metrics",
        incremental_strategy="merge",
        unique_key="date",
        on_schema_change="append_new_columns",
        merge_update_columns=var("backfill_columns", []),
        merge_exclude_columns=["created_on"] if not var("backfill_columns", []) else none,
        full_refresh=var("full_refresh", false),
        tags=["ez_metrics"],
    )
}}

{% set backfill_date = var("backfill_date", None) %}

with
    fundamental_data as (
        select
            date
            , chain
            , coalesce(txns, 0) as txns
            , coalesce(daa, 0) as dau
            , coalesce(gas, 0) as fees_native
            , coalesce(gas_usd, 0) as fees
            , coalesce(fees / txns, 0) as avg_txn_fee
            , coalesce(revenue, 0) as revenue
            , coalesce(gas, 0) as revenue_native
        from {{ ref("fact_aptos_daa_txns_gas_gas_usd_revenue") }}
    )
    , price_data as ({{ get_coingecko_metrics("aptos") }})
    , defillama_data as ({{ get_defillama_metrics("aptos") }})
    , github_data as ({{ get_github_metrics("aptos") }})
    , rolling_metrics as ({{ get_rolling_active_address_metrics("aptos") }})
    , aptos_dex_volumes as (
        select 
            date
            , coalesce(volume_usd, 0) as dex_volumes
        from {{ ref("fact_aptos_dex_volumes") }}
    )
select
    fundamental_data.date
    , 'aptos' as artemis_id
    , fundamental_data.chain
    , txns
    , fees_native
    , fees
    , avg_txn_fee
    , revenue_native
    , revenue
    , aptos_dex_volumes.dex_volumes

    -- Standardized Metrics

    -- Market Data
    , price_data.price
    , price_data.market_cap
    , price_data.fdmc
    , price_data.token_volume

    -- Chain Metrics
    , fundamental_data.dau as chain_dau
    , fundamental_data.dau as dau
    , fundamental_data.wau as chain_wau
    , fundamental_data.mau as chain_mau
    , fundamental_data.txns as chain_txns
    , fundamental_data.txns as txns
    , fundamental_data.avg_txn_fee as chain_avg_txn_fee
    , aptos_dex_volumes.dex_volumes as chain_spot_volume
    , defillama_data.tvl as chain_tvl
    , defillama_data.tvl as tvl

    -- Fee Data
    , fundamental_data.fees as chain_fees
    , fundamental_data.fees as burned_fee_allocation
    , fundamental_data.fees_native as burned_fee_allocation_native

    -- Financial Metrics
    , fundamental_data.revenue as revenue
    , fundamental_data.revenue_native as revenue_native

    -- Developer Metrics
    , github_data.weekly_commits_core_ecosystem
    , github_data.weekly_commits_sub_ecosystem
    , github_data.weekly_developers_core_ecosystem
    , github_data.weekly_developers_sub_ecosystem

    -- Turnover Metrics
    , price_data.token_turnover_circulating
    , price_data.token_turnover_fdv

    -- timestamp columns
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as created_on
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as modified_on

from fundamental_data
left join price_data on fundamental_data.date = price_data.date
left join defillama_data on fundamental_data.date = defillama_data.date
left join github_data on fundamental_data.date = github_data.date
left join rolling_metrics on fundamental_data.date = rolling_metrics.date
left join aptos_dex_volumes on fundamental_data.date = aptos_dex_volumes.date
where true
{{ ez_metrics_incremental("fundamental_data.date", backfill_date) }}
and fundamental_data.date < to_date(sysdate())
