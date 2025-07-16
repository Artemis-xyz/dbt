-- depends_on {{ ref('fact_solana_transactions_v2') }}
{{
    config(
        materialized="table",
        unique_key="date",
        snowflake_warehouse="SOLANA",
        database="solana",
        schema="core",
        alias="ez_metrics",
        on_schema_change='append_new_columns'
    )
}}

with
    contract_data as ({{ get_contract_metrics("solana") }}),
    stablecoin_data as ({{ get_stablecoin_metrics("solana") }}),
    defillama_data as ({{ get_defillama_metrics("solana") }}),
    github_data as ({{ get_github_metrics("solana") }}),
    price as ({{ get_coingecko_metrics("solana") }}),
    staking_data as ({{ get_staking_metrics("solana") }}),
    issuance_data as (
        select date, chain, issuance from {{ ref("fact_solana_issuance_silver") }}
    ),
    nft_metrics as ({{ get_nft_metrics("solana") }}),
    p2p_metrics as ({{ get_p2p_metrics("solana") }}),
    rolling_metrics as ({{ get_rolling_active_address_metrics("solana") }}),
    fundamental_usage as (
        select
            date,
            gas,
            gas_usd,
            median_txn_fee,
            base_fee_native,
            txns,
            dau,
            returning_users,
            new_users,
            vote_tx_fee_native
        from {{ ref('fact_solana_fundamental_data') }}
    ), 
    solana_dex_volumes as (
        select date, daily_volume_usd as dex_volumes
        from {{ ref("fact_solana_dex_volumes") }}
    )
    , jito_tips as (
        SELECT
            day as date,
            tip_fees
        FROM {{ ref('fact_jito_dau_txns_fees')}}
    )
    , supply_data as (
        select date, issued_supply, circulating_supply
        from {{ ref('fact_solana_supply_data') }}
    )
    , total_economic_activity as (
        select date, total_economic_activity
        from SOLANA.PROD_RAW.EZ_SOLANA_TEA
    )
select
    coalesce(fundamental_usage.date, supply_data.date) as date
    , 'solana' as chain
    , txns
    , dau
    , wau
    , mau
    , gas_usd / txns as avg_txn_fee
    , median_txn_fee
    , issuance
    , nft_trading_volume
    , solana_dex_volumes.dex_volumes as dex_volumes
    -- Standardzed metrics
    -- Market Data
    , price
    , market_cap
    , fdmc
    , tvl

    -- Chain Usage Metrics
    , txns AS chain_txns
    , dau AS chain_dau
    , wau AS chain_wau
    , mau AS chain_mau
    , returning_users
    , new_users
    , gas_usd / txns as chain_avg_txn_fee
    , median_txn_fee AS chain_median_txn_fee
    , total_staked_native
    , total_staked_usd as total_staked
    , nft_trading_volume AS chain_nft_trading_volume
    , p2p_native_transfer_volume
    , p2p_token_transfer_volume
    , p2p_transfer_volume
    , coalesce(solana_dex_volumes.dex_volumes, 0) + coalesce(nft_trading_volume, 0) + coalesce(p2p_transfer_volume, 0) as settlement_volume
    , solana_dex_volumes.dex_volumes as chain_spot_volume
    , case
        when (gas - base_fee_native) < 0.00001 then 0 else (gas - base_fee_native)
    end as priority_fees_native
    , case
        when (gas_usd - base_fee_native * price ) < 0.001 then 0 else (gas_usd - base_fee_native * price )
    end as priority_fees
    , total_economic_activity

    -- Cashflow Metrics
    , gas_usd + vote_tx_fee_native * price as chain_fees
    , gas + vote_tx_fee_native as fees_native
    , vote_tx_fee_native * price + gas_usd as fees
    , IFF(fundamental_usage.date < '2025-02-13', fees_native * .5, ((base_fee_native + vote_tx_fee_native) * .5) + priority_fee_native) as validator_fee_allocation_native
    , IFF(fundamental_usage.date < '2025-02-13', fees * .5, ((base_fee_native * price  + vote_tx_fee_native * price) * .5) + priority_fee) as validator_fee_allocation
    , IFF(fundamental_usage.date < '2025-02-13', fees_native * .5, (base_fee_native + vote_tx_fee_native) * .5) as burned_fee_allocation_native
    , IFF(fundamental_usage.date < '2025-02-13', fees * .5, (base_fee_native * price  + vote_tx_fee_native * price) * .5) as burned_fee_allocation
    , base_fee_native
    , base_fee_native * price AS base_fee
    , vote_tx_fee_native
    , vote_tx_fee_native * price AS vote_tx_fee
    , chain_fees + COALESCE(jito_tips.tip_fees, 0) as rev -- Blockworks' REV
    

    -- Financial Statement Metrics
    , IFF(fundamental_usage.date < '2025-02-13', fees_native * .5, (base_fee_native + vote_tx_fee_native) * .5) as revenue_native
    , IFF(fundamental_usage.date < '2025-02-13', fees * .5, (base_fee_native * price  + vote_tx_fee_native * price) * .5) as revenue
    , issuance * price as token_incentives
    , revenue - token_incentives as earnings
    
    -- Supply Metrics
    , issuance AS gross_emissions_native
    , issuance * price AS gross_emissions
    , issued_supply as issued_supply_native
    , circulating_supply as circulating_supply_native

    -- Developer Metrics
    , weekly_commits_core_ecosystem
    , weekly_commits_sub_ecosystem
    , weekly_developers_core_ecosystem
    , weekly_developers_sub_ecosystem
    , weekly_contracts_deployed
    , weekly_contract_deployers
    -- Stablecoin metrics
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
from fundamental_usage 
left join defillama_data on fundamental_usage.date = defillama_data.date
left join stablecoin_data on fundamental_usage.date = stablecoin_data.date
left join price on fundamental_usage.date = price.date
left join github_data on fundamental_usage.date = github_data.date
left join contract_data on fundamental_usage.date = contract_data.date
left join staking_data on fundamental_usage.date = staking_data.date
left join issuance_data on fundamental_usage.date = issuance_data.date
left join nft_metrics on fundamental_usage.date = nft_metrics.date
left join p2p_metrics on fundamental_usage.date = p2p_metrics.date
left join rolling_metrics on fundamental_usage.date = rolling_metrics.date
left join solana_dex_volumes on fundamental_usage.date = solana_dex_volumes.date
left join jito_tips on fundamental_usage.date = jito_tips.date
left join supply_data on fundamental_usage.date = supply_data.date
left join total_economic_activity on fundamental_usage.date = total_economic_activity.date
where fundamental_usage.date < to_date(sysdate())
