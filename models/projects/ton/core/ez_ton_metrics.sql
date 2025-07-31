{{
    config(
        materialized="incremental",
        snowflake_warehouse="TON",
        database="ton",
        schema="core",
        alias="ez_metrics",
        incremental_strategy="merge",
        unique_key="date",
        on_schema_change="append_new_columns",
        merge_update_columns=var("backfill_columns", []),
        merge_exclude_columns=["created_on"] if not var("backfill_columns", []) else none,
        full_refresh=var("full_refresh", false),
        tags=["ez_metrics"]
    )
}}

{% set backfill_date = var("backfill_date", None) %}

with
    fundamental_data as (
        select 
            date
            , coalesce(txns, 0) as transaction_nodes
        from {{ ref("fact_ton_daa_txns_gas_gas_usd_revenue_revenue_native") }}
    ), 
    ton_apps_fundamental_data as (
        select 
            date
            , coalesce(dau, 0) as dau
            , coalesce(fees_native, 0) as fees_native
            , coalesce(txns, 0) as txns
            , coalesce(avg_txn_fee_native, 0) as avg_txn_fee_native
        from {{ ref("fact_ton_fundamental_metrics") }}
    )
    , market_metrics as ({{ get_coingecko_metrics("the-open-network") }})
    , defillama_data as ({{ get_defillama_metrics("ton") }})
    , stablecoin_data as ({{ get_stablecoin_metrics("ton") }})
    , github_data as ({{ get_github_metrics("ton") }})
    , rolling_metrics as ({{ get_rolling_active_address_metrics("ton") }})
    , block_rewards_data as (
        select date, coalesce(block_rewards_native, 0) as block_rewards_native
        from {{ ref("fact_ton_minted") }}
    )
    , supply_data as (
        select 
            date
            , premine_unlocks_native
            , gross_emissions_native
            , burns_native
            , net_supply_change_native
            , max_supply_native
            , total_supply_native
            , foundation_owned
            , issued_supply_native
            , unvested_tokens
            , circulating_supply_native
        from {{ ref("fact_ton_supply_data") }}
    )
select
    supply.date
    , 'ton' as artemis_id
    , 'ton' as chain
    
    -- Standardized Metrics

    -- Market Data
    , market_metrics.price
    , market_metrics.market_cap
    , market_metrics.fdmc
    , market_metrics.token_volume
    

    -- Usage Data
    , ton_apps_fundamental_data.dau as chain_dau
    , ton_apps_fundamental_data.dau as dau
    , ton_apps_fundamental_data.wau as wau
    , ton_apps_fundamental_data.mau as mau
    , ton_apps_fundamental_data.txns as chain_txns
    , ton_apps_fundamental_data.txns as txns
    , ton_apps_fundamental_data.avg_txn_fee_native * price AS chain_avg_txn_fee

    -- Fee Data
    , ton_apps_fundamental_data.fees_native as fees_native
    , ton_apps_fundamental_data.fees as fees
    , ton_apps_fundamental_data.fees_native / 2 as burned_fee_allocation_native
    , ton_apps_fundamental_data.fees_native / 2 * market_metrics.price as burned_fee_allocation
    , ton_apps_fundamental_data.fees_native / 2 as validator_fee_allocation_native
    , ton_apps_fundamental_data.fees_native / 2 * market_metrics.price as validator_fee_allocation

    -- Financial Statement Metrics
    , ton_apps_fundamental_data.fees_native / 2 as revenue_native
    , ton_apps_fundamental_data.fees / 2 as revenue
    
    -- Developer Metrics
    , github_data.weekly_commits_core_ecosystem
    , github_data.weekly_commits_sub_ecosystem
    , github_data.weekly_developers_core_ecosystem
    , github_data.weekly_developers_sub_ecosystem
    
    -- Supply Metrics
    , block_rewards_data.block_rewards_native * market_metrics.price as gross_emissions
    , block_rewards_data.block_rewards_native as gross_emissions_native
    , supply_data.max_supply_native
    , supply_data.burns_native
    , supply_data.burns_native * market_metrics.price as burns
    , supply_data.total_supply_native
    , supply_data.issued_supply_native
    , supply_data.premine_unlocks_native
    , supply_data.circulating_supply_native

    -- Stablecoin Metrics
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
    , stablecoin_data.p2p_stablecoin_txns
    , stablecoin_data.p2p_stablecoin_dau
    , stablecoin_data.p2p_stablecoin_mau
    , stablecoin_data.p2p_stablecoin_transfer_volume
    , stablecoin_data.p2p_stablecoin_tokenholder_count

    -- Token Turnover/Other Data
    , market_metrics.token_turnover_circulating
    , market_metrics.token_turnover_fdv

    -- Bespoke Metrics
    , fundamental_data.transaction_nodes

    -- timestamp columns
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as created_on
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as modified_on

from supply_data as supply
left join ton_apps_fundamental_data as ton on supply.date = ton.date
left join market_metrics on supply.date = market_metrics.date
left join defillama_data on supply.date = defillama_data.date
left join github_data on supply.date = github_data.date
left join fundamental_data on supply.date = fundamental_data.date
left join stablecoin_data on supply.date = stablecoin_data.date
left join rolling_metrics on supply.date = rolling_metrics.date
left join block_rewards_data on supply.date = block_rewards_data.date
where true
{{ ez_metrics_incremental('supply.date', backfill_date) }}
and supply.date < to_date(sysdate())
