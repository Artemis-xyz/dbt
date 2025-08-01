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
        full_refresh=false,
        tags=["ez_metrics"]
    )
}}

{% set backfill_date = var("backfill_date", None) %}

with
    fundamental_data as (
        select 
            date,
            txns as transaction_nodes
        from {{ ref("fact_ton_daa_txns_gas_gas_usd_revenue_revenue_native") }}
    ), 
    ton_apps_fundamental_data as (
        select 
            date
            , dau
            , fees_native
            , txns
            , avg_txn_fee_native
        from {{ ref("fact_ton_fundamental_metrics") }}
    ),
    price_data as ({{ get_coingecko_metrics("the-open-network") }}),
    defillama_data as ({{ get_defillama_metrics("ton") }}),
    stablecoin_data as ({{ get_stablecoin_metrics("ton", backfill_date="2019-11-15") }}),
    github_data as ({{ get_github_metrics("ton") }}),
    rolling_metrics as ({{ get_rolling_active_address_metrics("ton") }})
    , block_rewards_data as (
        select date, block_rewards_native
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
    , 'ton' as chain
    , coalesce(dau, 0) as dau
    , coalesce(wau, 0) as wau
    , coalesce(mau, 0) as mau
    , coalesce(txns, 0) as txns
    , coalesce(fees_native, 0) as fees_native
    , coalesce(fees_native, 0) * price AS fees
    , coalesce(fees_native, 0) / 2 AS revenue_native
    , (coalesce(fees_native, 0) / 2) * price AS revenue
    , coalesce(avg_txn_fee_native, 0) * price AS avg_txn_fee
    -- Bespoke Metrics
    , coalesce(transaction_nodes, 0) as transaction_nodes
    -- Standardized Metrics
    -- Market Data Metrics
    , price
    , market_cap
    , fdmc
    , tvl
    -- Chain Usage Metrics
    , coalesce(dau, 0) AS chain_dau
    , coalesce(wau, 0) AS chain_wau
    , coalesce(mau, 0) AS chain_mau
    , coalesce(txns, 0) AS chain_txns
    , coalesce(avg_txn_fee_native, 0) * price AS chain_avg_txn_fee
    -- Cash Flow Metrics
    , coalesce(fees, 0) * price as chain_fees
    , coalesce(fees_native, 0) AS ecosystem_revenue_native
    , coalesce(fees, 0) * price AS ecosystem_revenue
    , coalesce(fees_native, 0) / 2 AS burned_fee_allocation_native
    , (coalesce(fees_native, 0) / 2) * price AS burned_fee_allocation
    , coalesce(fees_native, 0) / 2 AS validator_fee_allocation_native
    , (coalesce(fees_native, 0) / 2) * price AS validator_fee_allocation
    -- Developer Metrics
    , weekly_commits_core_ecosystem
    , weekly_commits_sub_ecosystem
    , weekly_developers_core_ecosystem
    , weekly_developers_sub_ecosystem
    -- Supply Metrics
    , premine_unlocks_native
    , gross_emissions_native
    , block_rewards_native * price AS gross_emissions
    , burns_native
    , net_supply_change_native
    , max_supply_native
    , total_supply_native
    , foundation_owned
    , issued_supply_native
    , unvested_tokens
    , circulating_supply_native
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
    , p2p_stablecoin_txns
    , p2p_stablecoin_dau
    , p2p_stablecoin_mau
    , p2p_stablecoin_transfer_volume
    , p2p_stablecoin_tokenholder_count
    -- timestamp columns
    , sysdate() as created_on
    , sysdate() as modified_on
from supply_data as supply
left join ton_apps_fundamental_data as ton on supply.date = ton.date
left join price_data on supply.date = price_data.date
left join defillama_data on supply.date = defillama_data.date
left join github_data on supply.date = github_data.date
left join fundamental_data on supply.date = fundamental_data.date
left join stablecoin_data on supply.date = stablecoin_data.date
left join rolling_metrics on supply.date = rolling_metrics.date
left join block_rewards_data on supply.date = block_rewards_data.date
where true
{{ ez_metrics_incremental('supply.date', backfill_date) }}
and supply.date < to_date(sysdate())
