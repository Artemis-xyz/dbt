-- depends_on {{ ref("fact_avalanche_transactions_v2") }}
-- depends_on {{ ref('fact_avalanche_amount_staked_silver') }}
{{
    config(
        materialized="incremental",
        snowflake_warehouse="AVALANCHE",
        database="avalanche",
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

with fundamental_data as (
    select * from {{ ref("fact_avalanche_fundamental_data") }}
    {{ ez_metrics_incremental("date", backfill_date) }}
)
, price_data as ({{ get_coingecko_metrics("avalanche-2") }})
, defillama_data as ({{ get_defillama_metrics("avalanche") }})
, stablecoin_data as ({{ get_stablecoin_metrics("avalanche") }})
, staking_data as ({{ get_staking_metrics("avalanche") }})
, github_data as ({{ get_github_metrics("avalanche") }})
, contract_data as ({{ get_contract_metrics("avalanche") }})
, issuance_data as (
    select date, validator_rewards as issuance
    from {{ ref("fact_avalanche_validator_rewards_silver") }}
    {{ ez_metrics_incremental("date", backfill_date) }}
)
, nft_metrics as ({{ get_nft_metrics("avalanche") }})
, p2p_metrics as ({{ get_p2p_metrics("avalanche") }})
, rolling_metrics as ({{ get_rolling_active_address_metrics("avalanche") }})
, bridge_volume_metrics as (
    select date, bridge_volume
    from {{ ref("fact_avalanche_bridge_bridge_volume") }}
    {{ ez_metrics_incremental("date", backfill_date) }}
    and chain is null
)
, bridge_daa_metrics as (
    select date, bridge_daa
    from {{ ref("fact_avalanche_bridge_bridge_daa") }}
    {{ ez_metrics_incremental("date", backfill_date) }}
)
, avalanche_c_dex_volumes as (
    select date, daily_volume as dex_volumes, daily_volume_adjusted as adjusted_dex_volumes
    from {{ ref("fact_avalanche_c_daily_dex_volumes") }}
    {{ ez_metrics_incremental("date", backfill_date) }}
)
, issued_supply_metrics as (
    select 
        date,
        max_supply as max_supply_native,
        total_supply as total_supply_native,
        issued_supply as issued_supply_native,
        circulating_supply_native as circulating_supply_native
    from {{ ref('fact_avalanche_issued_supply_and_float') }}
    {{ ez_metrics_incremental("date", backfill_date) }}
)
, date_spine as (
    SELECT date
    FROM {{ ref("dim_date_spine") }}
    WHERE date between '2014-04-13' AND to_date(sysdate()) -- Dev data goes back to 2014
)
, application_fees AS (
    SELECT 
        DATE_TRUNC(DAY, date) AS date 
        , SUM(COALESCE(fees, 0)) AS application_fees
    FROM {{ ref("ez_protocol_datahub_by_chain") }}
    {{ ez_metrics_incremental("DATE_TRUNC(DAY, date)", backfill_date) }}
        AND chain = 'avalanche'
    GROUP BY 1
)

select
    staking_data.date
    , coalesce(fundamental_data.chain, 'avalanche') as chain
    , txns
    , dau
    , wau
    , mau
    , fees_native
    , case when fees is null then fees_native * price else fees end as fees
    , avg_txn_fee
    , median_txn_fee
    , fees_native as revenue_native
    , fees as revenue
    , dau_over_100
    , dune_dex_volumes_avalanche_c.dex_volumes
    , dune_dex_volumes_avalanche_c.adjusted_dex_volumes
    , nft_trading_volume
    , total_staked_usd
    , issuance

    -- Standardized Metrics

    -- Market Data Metrics
    , price
    , market_cap
    , fdmc
    , tvl

    -- Chain Usage Metrics
    , dau AS chain_dau
    , wau AS chain_wau
    , mau AS chain_mau
    , txns AS chain_txns
    , avg_txn_fee AS chain_avg_txn_fee
    , median_txn_fee AS chain_median_txn_fee
    , returning_users
    , new_users
    , sybil_users
    , non_sybil_users
    , low_sleep_users
    , high_sleep_users
    , dau_over_100 AS dau_over_100_balance
    , total_staked_native AS total_staked_native
    , total_staked_usd AS total_staked
    , dune_dex_volumes_avalanche_c.dex_volumes AS chain_spot_volume
    , nft_trading_volume AS chain_nft_trading_volume
    , p2p_native_transfer_volume
    , p2p_token_transfer_volume
    , p2p_transfer_volume
    , coalesce(artemis_stablecoin_transfer_volume, 0) - coalesce(stablecoin_data.p2p_stablecoin_transfer_volume, 0) as non_p2p_stablecoin_transfer_volume
    , coalesce(dune_dex_volumes_avalanche_c.dex_volumes, 0) + coalesce(nft_trading_volume, 0) + coalesce(p2p_transfer_volume, 0) as settlement_volume
    , coalesce(fees, 0) + coalesce(settlement_volume, 0) + coalesce(application_fees.application_fees, 0) as total_economic_activity

    -- Cash Flow Metrics
    , case when fees is null then fees_native * price else fees end as chain_fees
    , fees_native AS ecosystem_revenue_native
    , case when fees is null then fees_native * price else fees end as ecosystem_revenue
    , fees_native AS burned_fee_allocation_native
    , case when fees is null then fees_native * price else fees end as burned_fee_allocation

    -- Issued Supply Metrics
    , issued_supply_metrics.max_supply_native
    , issued_supply_metrics.total_supply_native
    , issued_supply_metrics.issued_supply_native
    , issued_supply_metrics.circulating_supply_native

    -- Supply Metrics
    , issuance AS emissions_native
    , issuance * price AS gross_emissions

    -- Developer Metrics
    , weekly_commits_core_ecosystem
    , weekly_commits_sub_ecosystem
    , weekly_developers_core_ecosystem
    , weekly_developers_sub_ecosystem
    , weekly_contracts_deployed
    , weekly_contract_deployers

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

    -- Bridge Metrics
    , bridge_volume
    , bridge_daa

    -- timestamp columns
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as created_on
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as modified_on

from staking_data
left join fundamental_data on staking_data.date = fundamental_data.date
left join price_data on staking_data.date = price_data.date
left join defillama_data on staking_data.date = defillama_data.date
left join stablecoin_data on staking_data.date = stablecoin_data.date
left join github_data on staking_data.date = github_data.date
left join contract_data on staking_data.date = contract_data.date
left join issuance_data on staking_data.date = issuance_data.date
left join nft_metrics on staking_data.date = nft_metrics.date
left join p2p_metrics on staking_data.date = p2p_metrics.date
left join rolling_metrics on staking_data.date = rolling_metrics.date
left join bridge_volume_metrics on staking_data.date = bridge_volume_metrics.date
left join bridge_daa_metrics on staking_data.date = bridge_daa_metrics.date
left join avalanche_c_dex_volumes as dune_dex_volumes_avalanche_c on staking_data.date = dune_dex_volumes_avalanche_c.date
left join issued_supply_metrics on staking_data.date = issued_supply_metrics.date
left join application_fees on staking_data.date = application_fees.date
{{ ez_metrics_incremental("staking_data.date", backfill_date) }}
    and staking_data.date < to_date(sysdate())
