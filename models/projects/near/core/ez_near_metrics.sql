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
        merge_exclude_columns=["created_on"] if not var("backfill_columns", []) else none,
        full_refresh=var("full_refresh", false),
        tags=["ez_metrics"],
    )
}}

{% set backfill_date = var("backfill_date", None) %}

with
    fundamental_data as ({{ get_fundamental_data_for_chain("near", "v2") }}),
    market_metrics as ({{ get_coingecko_metrics("near") }}),
    defillama_data as ({{ get_defillama_metrics("near") }}),
    revenue_data as (
        select date, revenue_native, revenue from {{ ref("fact_near_revenue") }}
    ),
    github_data as ({{ get_github_metrics("near") }}),
    contract_data as ({{ get_contract_metrics("near") }}),
    p2p_metrics as ({{ get_p2p_metrics("near") }}),
    rolling_metrics as ({{ get_rolling_active_address_metrics("near") }}),
    da_metrics as (
        select date, blob_fees_native, blob_fees, blob_size_mib, avg_mib_per_second, avg_cost_per_mib_native, avg_cost_per_mib, submitters
        from {{ ref("fact_near_da_metrics") }}
    ), 
    near_dex_volumes as (
        select date, volume_usd as dex_volumes
        from {{ ref("fact_near_dex_volumes") }}
    ),
    application_fees AS (
    SELECT 
        DATE_TRUNC(DAY, date) AS date 
        , SUM(COALESCE(fees, 0)) AS application_fees
    FROM {{ ref("ez_protocol_datahub_by_chain") }}
    WHERE chain = 'near'
    GROUP BY 1
    )

select
    fundamental_data.date
    , 'near' as artemis_id
    , 'near' as chain
    
    -- Standardized Metrics

    -- Market Data
    , market_metrics.price
    , market_metrics.market_cap
    , market_metrics.fdmc
    , market_metrics.token_volume

    -- Usage Data
    , fundamental_data.dau as chain_dau
    , fundamental_data.dau as dau
    , fundamental_data.wau as chain_wau
    , fundamental_data.mau as chain_mau
    , fundamental_data.txns as chain_txns
    , fundamental_data.txns as txns
    , fundamental_data.avg_txn_fee AS chain_avg_txn_fee
    , fundamental_data.returning_users
    , fundamental_data.new_users
    , fundamental_data.low_sleep_users
    , fundamental_data.high_sleep_users
    , near_dex_volumes.dex_volumes as chain_spot_volume

    -- Fee Data
    , fundamental_data.fees_native as fees_native
    , fundamental_data.fees as fees
    , fundamental_data.fees as chain_fees
    , fundamental_data.median_txn_fee as chain_median_txn_fee

    -- Financial Statements
    , revenue_data.revenue_native as burned_fee_allocation_native
    , revenue_data.revenue as burned_fee_allocation

    -- Developer Metrics
    , github_data.weekly_commits_core_ecosystem
    , github_data.weekly_commits_sub_ecosystem
    , github_data.weekly_developers_core_ecosystem
    , github_data.weekly_developers_sub_ecosystem
    , contract_data.weekly_contracts_deployed
    , contract_data.weekly_contract_deployers

    -- Bespoke Metrics
    , p2p_metrics.p2p_native_transfer_volume
    , p2p_metrics.p2p_token_transfer_volume
    , p2p_metrics.p2p_stablecoin_transfer_volume
    , p2p_metrics.p2p_transfer_volume
    , da_metrics.blob_fees_native
    , da_metrics.blob_fees
    , da_metrics.blob_size_mib
    , da_metrics.avg_mib_per_second
    , da_metrics.avg_cost_per_mib_native
    , da_metrics.avg_cost_per_mib
    , submitters
    , chain_fees + blob_fees + p2p_transfer_volume + near_dex_volumes.dex_volumes + application_fees.application_fees as total_economic_activity

    -- Other Data
    , market_metrics.token_turnover_circulating
    , market_metrics.token_turnover_fdv

    -- timestamp columns
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as created_on
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as modified_on

from fundamental_data
left join market_metrics on fundamental_data.date = market_metrics.date
left join defillama_data on fundamental_data.date = defillama_data.date
left join revenue_data on fundamental_data.date = revenue_data.date
left join github_data on fundamental_data.date = github_data.date
left join contract_data on fundamental_data.date = contract_data.date
left join p2p_metrics on fundamental_data.date = p2p_metrics.date
left join rolling_metrics on fundamental_data.date = rolling_metrics.date
left join da_metrics on fundamental_data.date = da_metrics.date
left join near_dex_volumes on fundamental_data.date = near_dex_volumes.date
left join application_fees on fundamental_data.date = application_fees.date
where true
{{ ez_metrics_incremental('fundamental_data.date', backfill_date) }}
and fundamental_data.date < to_date(sysdate())
