-- depends_on {{ ref("fact_near_transactions_v2") }}
{{
    config(
        materialized="incremental",
        snowflake_warehouse="NEAR",
        database="near",
        schema="core",
        alias="ez_metrics",
        incremental_strategy="merge",
        unique_key="date",
        on_schema_change="append_new_columns",
        merge_update_columns=var("backfill_columns", []),
        merge_exclude_columns=["created_on"] | reject('in', var("backfill_columns", [])) | list,
        full_refresh=false,
        tags=["ez_metrics"],
    )
}}

{% set backfill_date = var("backfill_date", None) %}

with
    fundamental_data as ({{ get_fundamental_data_for_chain("near", "v2") }}),
    price_data as ({{ get_coingecko_metrics("near") }}),
    defillama_data as ({{ get_defillama_metrics("near") }}),
    revenue_data as (
        select date, revenue_native, revenue from {{ ref("fact_near_revenue") }}
        {{ ez_metrics_incremental('date', backfill_date) }}
    ),
    github_data as ({{ get_github_metrics("near") }}),
    contract_data as ({{ get_contract_metrics("near") }}),
    p2p_metrics as ({{ get_p2p_metrics("near") }}),
    rolling_metrics as ({{ get_rolling_active_address_metrics("near") }}),
    da_metrics as (
        select date, blob_fees_native, blob_fees, blob_size_mib, avg_mib_per_second, avg_cost_per_mib_native, avg_cost_per_mib, submitters
        from {{ ref("fact_near_da_metrics") }}
        {{ ez_metrics_incremental('date', backfill_date) }}
    ), 
    near_dex_volumes as (
        select date, volume_usd as dex_volumes
        from {{ ref("fact_near_dex_volumes") }}
        {{ ez_metrics_incremental('date', backfill_date) }}
    ),
    application_fees AS (
    SELECT 
        DATE_TRUNC(DAY, date) AS date 
        , SUM(COALESCE(fees, 0)) AS application_fees
    FROM {{ ref("ez_protocol_datahub_by_chain") }}
    {{ ez_metrics_incremental('date', backfill_date) }}
    AND chain = 'near'
    GROUP BY 1
    )

select
    fundamental_data.date
    , fundamental_data.chain
    , txns
    , dau
    , wau
    , mau
    , fees_native
    , fees
    , avg_txn_fee
    , median_txn_fee
    , revenue_native
    , revenue
    , coalesce(near_dex_volumes.dex_volumes, 0) as dex_volumes
    -- Standardized Metrics
    -- Market Data Metrics
    , price
    , market_cap
    , fdmc
    , tvl
    -- Chain Usage Metrics
    , dau as chain_dau
    , wau as chain_wau
    , mau as chain_mau
    , txns as chain_txns
    , avg_txn_fee AS chain_avg_txn_fee
    , returning_users
    , new_users
    , low_sleep_users
    , high_sleep_users
    -- Cashflow Metrics
    , case when fees is null then fees_native * price else fees end as chain_fees
    , fees_native as ecosystem_revenue_native
    , case when fees is null then fees_native * price else fees end as ecosystem_revenue
    , median_txn_fee AS chain_median_txn_fee
    , revenue_native AS burned_fee_allocation_native
    , revenue AS burned_fee_allocation
    -- Developer Metrics
    , weekly_commits_core_ecosystem
    , weekly_commits_sub_ecosystem
    , weekly_developers_core_ecosystem
    , weekly_developers_sub_ecosystem
    , weekly_contracts_deployed
    , weekly_contract_deployers
    , p2p_native_transfer_volume
    , p2p_token_transfer_volume
    , p2p_stablecoin_transfer_volume
    , p2p_transfer_volume
    , blob_fees_native
    , blob_fees
    , blob_size_mib
    , avg_mib_per_second
    , avg_cost_per_mib_native
    , avg_cost_per_mib
    , submitters
    , coalesce(near_dex_volumes.dex_volumes, 0) as chain_spot_volume
    , coalesce(chain_fees, 0) + coalesce(blob_fees, 0) + coalesce(p2p_transfer_volume, 0) + coalesce(near_dex_volumes.dex_volumes, 0) + coalesce(application_fees.application_fees, 0) as total_economic_activity
    -- timestamp columns
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as created_on
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as modified_on
from fundamental_data
left join price_data on fundamental_data.date = price_data.date
left join defillama_data on fundamental_data.date = defillama_data.date
left join revenue_data on fundamental_data.date = revenue_data.date
left join github_data on fundamental_data.date = github_data.date
left join contract_data on fundamental_data.date = contract_data.date
left join p2p_metrics on fundamental_data.date = p2p_metrics.date
left join rolling_metrics on fundamental_data.date = rolling_metrics.date
left join da_metrics on fundamental_data.date = da_metrics.date
left join near_dex_volumes on fundamental_data.date = near_dex_volumes.date
left join application_fees on fundamental_data.date = application_fees.date
{{ ez_metrics_incremental('fundamental_data.date', backfill_date) }}
and fundamental_data.date < to_date(sysdate())
