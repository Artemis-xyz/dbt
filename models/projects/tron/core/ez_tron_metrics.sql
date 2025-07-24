-- depends_on {{ ref("fact_tron_transactions_v2") }}
{{
    config(
        materialized="incremental",
        snowflake_warehouse="BAM_TRANSACTION_XLG",
        database="tron",
        schema="core",
        alias="ez_metrics",
        incremental_strategy="merge",
        unique_key="date",
        on_schema_change="append_new_columns",
        merge_update_columns=var("backfill_columns", []),
        merge_exclude_columns=["created_on"] | reject('in', var("backfill_columns", [])) | list,
        full_refresh=false,
        tags=["ez_metrics"]
    )
}}

{% set backfill_date = var("backfill_date", None) %}

with fundamental_data as (
    {{ get_fundamental_data_for_chain("tron", "v2") }}
)
, market_metrics as ({{ get_coingecko_metrics("tron") }})
, defillama_data as ({{ get_defillama_metrics("tron") }})
, stablecoin_data as ({{ get_stablecoin_metrics("tron") }})
, github_data as ({{ get_github_metrics("tron") }})
, p2p_metrics as ({{ get_p2p_metrics("tron") }})
, rolling_metrics as ({{ get_rolling_active_address_metrics("tron") }})
, token_incentives as (
    select 
        date,
        sum(token_incentives) as token_incentives,
    from {{ ref("fact_tron_token_incentives") }}
    {{ ez_metrics_incremental('date', backfill_date) }}
    group by date
)
, issued_supply_metrics as (
    select
        date,
        max_supply_to_date as max_supply_native,
        total_supply as total_supply_native,
        issued_supply as issued_supply_native,
        floating_supply as circulating_supply_native,
    from {{ ref("fact_tron_issued_supply_and_float") }}
    {{ ez_metrics_incremental('date', backfill_date) }}
)
, application_fees AS (
    SELECT 
        DATE_TRUNC(DAY, date) AS date 
        , SUM(COALESCE(fees, 0)) AS application_fees
    FROM {{ ref("ez_protocol_datahub_by_chain") }}
    {{ ez_metrics_incremental('date', backfill_date) }}
    AND chain = 'tron'
    GROUP BY 1
)

select
    coalesce(
        fundamental_data.date,
        market_metrics.date,
        defillama_data.date,
        stablecoin_data.date,
        github_data.date
    ) as date
    , 'tron' as chain

    --Old Metrics needed for backwards compatibility
    , txns
    , dau
    , wau
    , mau
    , fees_native
    , fees_native AS revenue_native
    , avg_txn_fee
    , median_txn_fee
    , dex_volumes

    -- Standardized Metrics

    -- Market Metrics
    , market_metrics.price
    , market_metrics.market_cap
    , market_metrics.fdmc
    
    -- Chain Usage Metrics
    , dau AS chain_dau
    , wau AS chain_wau
    , mau AS chain_mau
    , tvl
    , txns AS chain_txns
    , avg_txn_fee AS chain_avg_txn_fee
    , median_txn_fee AS chain_median_txn_fee
    , returning_users
    , new_users
    , p2p_native_transfer_volume
    , p2p_token_transfer_volume
    , p2p_transfer_volume
    , dex_volumes AS chain_spot_volume
    , coalesce(artemis_stablecoin_transfer_volume, 0) - coalesce(stablecoin_data.p2p_stablecoin_transfer_volume, 0) as non_p2p_stablecoin_transfer_volume
    , coalesce(dex_volumes, 0) + coalesce(p2p_transfer_volume, 0) as settlement_volume

    -- Cash Flow Metrics
    , fees as chain_fees
    , fees_native AS burned_fee_allocation_native
    , fees AS burned_fee_allocation

    -- TEA
    , coalesce(fees, 0) + coalesce(settlement_volume, 0) + coalesce(application_fees.application_fees, 0) as total_economic_activity

    -- Financial Statement Metrics
    , fees as fees
    , burned_fee_allocation as revenue
    , token_incentives.token_incentives as token_incentives
    , revenue - token_incentives as earnings

    -- Issued Supply Metrics
    , issued_supply_metrics.max_supply_native
    , issued_supply_metrics.total_supply_native
    , issued_supply_metrics.issued_supply_native
    , issued_supply_metrics.circulating_supply_native

    -- Developer Metrics
    , weekly_commits_core_ecosystem
    , weekly_commits_sub_ecosystem
    , weekly_developers_core_ecosystem
    , weekly_developers_sub_ecosystem

    -- Stablecoin Metrics
    , stablecoin_total_supply
    , stablecoin_txns
    , stablecoin_dau
    , stablecoin_mau
    , stablecoin_transfer_volume
    , stablecoin_tokenholder_count
    , artemis_stablecoin_txns
    , artemis_stablecoin_dau
    , artemis_stablecoin_mau
    , artemis_stablecoin_transfer_volume
    , p2p_stablecoin_tokenholder_count
    , p2p_stablecoin_txns
    , p2p_stablecoin_dau
    , p2p_stablecoin_mau
    , stablecoin_data.p2p_stablecoin_transfer_volume
    -- timestamp columns
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as created_on
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as modified_on
from fundamental_data
left join market_metrics on fundamental_data.date = market_metrics.date
left join defillama_data on fundamental_data.date = defillama_data.date
left join stablecoin_data on fundamental_data.date = stablecoin_data.date
left join github_data on fundamental_data.date = github_data.date
left join p2p_metrics on fundamental_data.date = p2p_metrics.date
left join rolling_metrics on fundamental_data.date = rolling_metrics.date
left join token_incentives on fundamental_data.date = token_incentives.date
left join issued_supply_metrics on fundamental_data.date = issued_supply_metrics.date
left join application_fees on fundamental_data.date = application_fees.date
{{ ez_metrics_incremental('fundamental_data.date', backfill_date) }}
and fundamental_data.date < to_date(sysdate())
