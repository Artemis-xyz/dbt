-- depends_on {{ ref("fact_polygon_transactions_v2") }}
{{
    config(
        materialized="incremental",
        snowflake_warehouse="polygon",
        database="polygon",
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

with
    date_spine as (
        select date
        from {{ ref("dim_date_spine") }}
        where date >= '2020-05-30' and date < to_date(sysdate())
    )
    , fundamental_data as ({{ get_fundamental_data_for_chain("polygon", "v2") }})
    , market_data as ({{ get_coingecko_metrics("polygon-ecosystem-token") }})
    , defillama_data as ({{ get_defillama_metrics("polygon") }})
    , stablecoin_data as ({{ get_stablecoin_metrics("polygon") }})
    , github_data as ({{ get_github_metrics("polygon") }})
    , contract_data as ({{ get_contract_metrics("polygon") }})
    , revenue_data as (
        select date, native_token_burn as revenue_native, revenue
        from {{ ref("agg_daily_polygon_revenue") }}
    )
    , l1_cost_data as (
        select
            raw_date as date,
            sum(tx_fee) as l1_data_cost_native,
            sum(gas_usd) as l1_data_cost
        from {{ref("fact_ethereum_transactions_v2")}}
        where lower(contract_address) = lower('0x86E4Dc95c7FBdBf52e33D563BbDB00823894C287')   
        group by date
    )
    , nft_metrics as ({{ get_nft_metrics("polygon") }})
    , p2p_metrics as ({{ get_p2p_metrics("polygon") }})
    , rolling_metrics as ({{ get_rolling_active_address_metrics("polygon") }})
    , bridge_volume_metrics as (
        select date, bridge_volume
        from {{ ref("fact_polygon_pos_bridge_bridge_volume") }}
        where chain is null
    )
    , bridge_daa_metrics as (
        select date, bridge_daa
        from {{ ref("fact_polygon_pos_bridge_bridge_daa") }}
    )
    , polygon_dex_volumes as (
        select date, daily_volume as dex_volumes, daily_volume_adjusted as adjusted_dex_volumes
        from {{ ref("fact_polygon_daily_dex_volumes") }}
    )
    , token_incentives as (
        select
            date,
            token_incentives
        from {{ref('fact_polygon_token_incentives')}}
    )

select
    date_spine.date
    , 'polygon' as artemis_id

    -- Standardized Metrics

    -- Market Data
    , market_data.price
    , market_data.market_cap
    , market_data.fdmc
    , market_data.token_volume

    -- Usage Data
    , fundamental_data.dau AS chain_dau
    , fundamental_data.dau
    , rolling_metrics.wau AS chain_wau
    , rolling_metrics.mau AS chain_mau
    , fundamental_data.txns AS chain_txns
    , fundamental_data.txns
    , defillama_data.tvl
    , nft_metrics.nft_trading_volume AS chain_nft_trading_volume
    , p2p_metrics.p2p_native_transfer_volume
    , p2p_metrics.p2p_token_transfer_volume
    , p2p_metrics.p2p_transfer_volume
    , polygon_dex_volumes.dex_volumes AS chain_spot_volume
    , coalesce(stablecoin_data.artemis_stablecoin_transfer_volume, 0) - coalesce(stablecoin_data.p2p_stablecoin_transfer_volume, 0) as non_p2p_stablecoin_transfer_volume
    , coalesce(polygon_dex_volumes.dex_volumes, 0) + coalesce(nft_trading_volume, 0) + coalesce(p2p_transfer_volume, 0) as settlement_volume

    -- Fee Data
    , fundamental_data.fees_native
    , fundamental_data.fees
    , fundamental_data.fees AS chain_fees
    , revenue_data.revenue AS burned_fee_allocation
    , l1_cost_data.l1_data_cost AS l1_fee_allocation
    , fundamental_data.fees - revenue_data.revenue - l1_cost_data.l1_data_cost AS validator_fee_allocation

    -- Financial Statement
    , revenue_data.revenue
    , ti.token_incentives
    , coalesce(revenue, 0) - coalesce(ti.token_incentives, 0) AS earnings

    -- Developer Metrics
    , github_data.weekly_commits_core_ecosystem
    , github_data.weekly_commits_sub_ecosystem
    , github_data.weekly_developers_core_ecosystem
    , github_data.weekly_developers_sub_ecosystem
    , contract_data.weekly_contracts_deployed
    , contract_data.weekly_contract_deployers

    -- Stablecoin Data
    , stablecoin_data.stablecoin_total_supply
    , stablecoin_data.stablecoin_txns
    , stablecoin_data.stablecoin_dau
    , stablecoin_data.stablecoin_mau
    , stablecoin_data.stablecoin_transfer_volume
    , stablecoin_data.stablecoin_tokenholder_count
    , stablecoin_data.artemis_stablecoin_txns
    , stablecoin_data.artemis_stablecoin_dau
    , stablecoin_data.artemis_stablecoin_mau
    , stablecoin_data.artemis_stablecoin_transfer_volume
    , stablecoin_data.p2p_stablecoin_tokenholder_count
    , stablecoin_data.p2p_stablecoin_txns
    , stablecoin_data.p2p_stablecoin_dau
    , stablecoin_data.p2p_stablecoin_mau
    , stablecoin_data.p2p_stablecoin_transfer_volume

    -- Bridge Data
    , bridge_volume_metrics.bridge_volume
    , bridge_daa_metrics.bridge_daa

    -- Bespoke Data
    , fundamental_data.returning_users
    , fundamental_data.new_users
    , fundamental_data.sybil_users
    , fundamental_data.non_sybil_users
    , fundamental_data.low_sleep_users
    , fundamental_data.high_sleep_users
    , fundamental_data.dau_over_100

    -- timestamp columns
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as created_on
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as modified_on
from date_spine
left join fundamental_data using (date)
left join market_data using (date)
left join defillama_data using (date)
left join stablecoin_data using (date)
left join github_data using (date)
left join contract_data using (date)
left join revenue_data using (date)
left join nft_metrics using (date)
left join p2p_metrics using (date)
left join rolling_metrics using (date)
left join l1_cost_data using (date)
left join bridge_volume_metrics using (date)
left join bridge_daa_metrics using (date)
left join polygon_dex_volumes using (date)
left join token_incentives ti using (date)
where true
{{ ez_metrics_incremental('fundamental_data.date', backfill_date) }}
and fundamental_data.date < to_date(sysdate())
