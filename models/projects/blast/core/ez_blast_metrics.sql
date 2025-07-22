-- depends_on {{ ref("fact_blast_transactions_v2") }}
{{
    config(
        materialized="incremental",
        snowflake_warehouse="blast",
        database="blast",
        schema="core",
        alias="ez_metrics",
        incremental_strategy="merge",
        unique_key="date",
        on_schema_change="append_new_columns",
        merge_exclude_columns=["created_on"],
        full_refresh=false
    )
}}

-- NOTE: When running a backfill, add merge_update_columns=[<columns>] to the config and set the backfill date below

{% set backfill_date = None %}

with
    fundamental_data as ({{ get_goldsky_chain_fundamental_metrics("blast", "_v2") }})
    , defillama_data as ({{ get_defillama_metrics("blast") }})
    , price_data as ({{ get_coingecko_metrics("blast") }})
    , eth_price as ({{ get_coingecko_metrics("ethereum") }})
    , contract_data as ({{ get_contract_metrics("blast") }})
    -- NOTE, this says l1 data cost, but that's inaccurate
    -- its both data and execution cost, but I'm following convention for now and we don't publish 
    -- this field anywhere, we only use it to derive revenue
    , expenses_data as (
        select date, chain, l1_data_cost_native, l1_data_cost
        from {{ ref("fact_blast_l1_data_cost") }}
        {{ ez_metrics_incremental("date", backfill_date) }}
    )  -- supply side revenue and fees
    , rolling_metrics as ({{ get_rolling_active_address_metrics("blast") }})
    , blast_dex_volumes as (
        select date, daily_volume as dex_volumes, daily_volume_adjusted as adjusted_dex_volumes
        from {{ ref("fact_blast_daily_dex_volumes") }}
        {{ ez_metrics_incremental("date", backfill_date) }}
    ),
    premine_emissions as (
        select date, premine_unlocks_native, circulating_supply_native
        from {{ ref("fact_blast_daily_supply_data") }}
        {{ ez_metrics_incremental("date", backfill_date) }}
    )
select
    DATE(coalesce(
        fundamental_data.date,
        defillama_data.date,
        contract_data.date,
        expenses_data.date
    )) as date
    , 'blast' as chain
    , txns
    , daa as dau
    , wau
    , mau
    , fees_native
    , fees_native * eth_price.price AS fees
    , fees / txns as avg_txn_fee
    , l1_data_cost_native
    , l1_data_cost
    , coalesce(fees_native, 0) - l1_data_cost_native as revenue_native  -- supply side: fees paid to squencer - fees paied to l1 (L2 Revenue)
    , coalesce(fees, 0) - l1_data_cost as revenue
    -- , dau_over_100 omitting balances for blast
    , dune_dex_volumes_blast.dex_volumes as dex_volumes
    , dune_dex_volumes_blast.adjusted_dex_volumes as adjusted_dex_volumes
    -- Standardized Metrics
    -- Market Data Metrics
    , price_data.price
    , price_data.market_cap
    , price_data.fdmc
    , tvl
    -- Chain Usage metrics
    , txns as chain_txns
    , dau AS chain_dau
    , wau AS chain_wau
    , mau AS chain_mau
    , avg_txn_fee AS chain_avg_txn_fee
    -- , returning_users
    -- , new_users
    -- , low_sleep_users
    -- , high_sleep_users
    -- , dau_over_100 AS dau_over_100_balance
    , dune_dex_volumes_blast.dex_volumes as chain_spot_volume
    -- Cashflow metrics
    , fees_native AS ecosystem_revenue_native
    , fees AS ecosystem_revenue
    , revenue_native AS burned_fee_allocation_native
    , revenue AS burned_fee_allocation
    , l1_data_cost_native AS l1_fee_allocation_native
    , l1_data_cost AS l1_fee_allocation
    , revenue_native AS foundation_fee_allocation_native
    , revenue AS foundation_fee_allocation
    -- Developer metrics
    , weekly_contracts_deployed
    , weekly_contract_deployers
    -- Supply Metrics
    , premine_unlocks_native
    , premine_unlocks_native as net_supply_change_native
    , circulating_supply_native
    -- timestamp columns
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as created_on
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as modified_on
from fundamental_data
left join defillama_data on fundamental_data.date = defillama_data.date
left join contract_data on fundamental_data.date = contract_data.date
left join expenses_data on fundamental_data.date = expenses_data.date
left join rolling_metrics on fundamental_data.date = rolling_metrics.date
left join blast_dex_volumes as dune_dex_volumes_blast on fundamental_data.date = dune_dex_volumes_blast.date
left join price_data on fundamental_data.date = price_data.date
left join eth_price on fundamental_data.date = eth_price.date
left join premine_emissions on fundamental_data.date = premine_emissions.date
{{ ez_metrics_incremental('fundamental_data.date', backfill_date) }}
    and fundamental_data.date < to_date(sysdate())
