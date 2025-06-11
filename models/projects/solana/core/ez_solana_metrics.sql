-- depends_on {{ ref('fact_solana_transactions_v2') }}
{{
    config(
        materialized="table",
        unique_key="date",
        snowflake_warehouse="SOLANA_XLG",
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
    {% if not is_incremental() %}
        unrefreshed_data as (
            select
                date_trunc('day', block_timestamp) as date,
                sum(case when index = 0 then fee / pow(10, 9) else 0 end) gas,
                median(case when index = 0 then fee / pow(10, 9) end) as median_txn_fee_native,
                sum(
                    case
                        when index = 0 then (array_size(signers) * (5000 / 1e9)) else 0
                    end
                ) as base_fee_native,
                count_if(index = 0) as txns,
                count(
                    distinct(case when succeeded = 'TRUE' then value else null end)
                ) dau,
                null as returning_users,
                null as new_users
            from
                solana_flipside.core.fact_transactions,
                lateral flatten(input => signers)
            where
                date_trunc('day', block_timestamp)
                < (select min(raw_date) from {{ ref('fact_solana_transactions_v2') }})
            group by date
        ),
        unrefreshed_data_with_price as (
            select
                unrefreshed_data.date,
                gas,
                gas * price as gas_usd,
                base_fee_native,
                txns,
                dau,
                returning_users,
                new_users,
                median_txn_fee_native * price as median_txn_fee
            from unrefreshed_data
            left join price on unrefreshed_data.date = price.date
        ),
    {% endif %}
    min_date as (
        select min(raw_date) as start_date, value as signer
        from {{ ref('fact_solana_transactions_v2') }}, lateral flatten(input => signers)
        where succeeded = 'TRUE'
        group by signer
    ),
    new_users as (
        select count(distinct signer) as new_users, start_date
        from min_date
        group by start_date
    ),
    voting_fees as (
        select
            date_trunc('day', block_timestamp) as date,
            sum(num_votes * 5000) / pow(10, 9) as vote_tx_fee_native
        from solana_flipside.gov.fact_votes_agg_block
        {% if is_incremental() %}
            where
                date_trunc('day', block_timestamp)
                > (select dateadd('day', -3, max(date)) from {{ this }})
        {% endif %}
        group by date
    ),
    agg_data as (
        select
            raw_date,
            max(chain) as chain,
            sum(case when index = 0 then tx_fee else 0 end) gas,
            sum(case when index = 0 then gas_usd else 0 end) gas_usd,
            median(case when index = 0 then gas_usd end) as median_txn_fee,
            sum(
                case when index = 0 then (array_size(signers) * (5000 / 1e9)) else 0 end
            ) as base_fee_native,
            count_if(index = 0) as txns,
            count(distinct(case when succeeded = 'TRUE' then value else null end)) dau
        from {{ ref('fact_solana_transactions_v2') }}, lateral flatten(input => signers)
        {% if is_incremental() %}
            where raw_date > (select dateadd('day', -3, max(date)) from {{ this }})
        {% endif %}
        group by raw_date
    ),
    fundamental_usage as (
        select
            agg_data.raw_date as date,
            gas,
            gas_usd,
            median_txn_fee,
            base_fee_native,
            txns,
            dau,
            (dau - new_users) as returning_users,
            new_users
        from agg_data
        left join new_users on date = new_users.start_date
        {% if not is_incremental() %}
            union
            select
                date,
                gas,
                gas_usd,
                median_txn_fee,
                base_fee_native,
                txns,
                dau,
                returning_users,
                new_users
            from unrefreshed_data_with_price
        {% endif %}
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
select
    fundamental_usage.date
    , 'solana' as chain
    , txns
    , dau
    , wau
    , mau
    , gas + vote_tx_fee_native as fees_native
    , vote_tx_fee_native * price + gas_usd as fees
    , gas_usd / txns as avg_txn_fee
    , median_txn_fee
    , IFF(fundamental_usage.date < '2025-02-13', fees_native * .5, (base_fee_native + vote_tx_fee_native) * .5) as revenue_native
    , IFF(fundamental_usage.date < '2025-02-13', fees * .5, (base_fee_native * price  + vote_tx_fee_native * price) * .5) as revenue
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
    end as priority_fee_native
    , case
        when (gas_usd - base_fee_native * price ) < 0.001 then 0 else (gas_usd - base_fee_native * price )
    end as priority_fee

    -- Cashflow Metrics
    , gas_usd + vote_tx_fee_native * price as chain_fees
    , gas + vote_tx_fee_native as ecosystem_revenue_native
    , gas_usd + vote_tx_fee_native * price as ecosystem_revenue
    , IFF(fundamental_usage.date < '2025-02-13', fees_native * .5, ((base_fee_native + vote_tx_fee_native) * .5) + priority_fee_native) as validator_fee_allocation_native
    , IFF(fundamental_usage.date < '2025-02-13', fees * .5, ((base_fee_native * price  + vote_tx_fee_native * price) * .5) + priority_fee) as validator_fee_allocation
    , IFF(fundamental_usage.date < '2025-02-13', fees_native * .5, (base_fee_native + vote_tx_fee_native) * .5) as burned_fee_allocation_native
    , IFF(fundamental_usage.date < '2025-02-13', fees * .5, (base_fee_native * price  + vote_tx_fee_native * price) * .5) as burned_fee_allocation
    , base_fee_native
    , base_fee_native * price AS base_fee
    , vote_tx_fee_native
    , vote_tx_fee_native * price AS vote_tx_fee
    , chain_fees + jito_tips.tip_fees as rev -- Blockworks' REV

    -- Financial Statement Metrics
    , chain_fees as fees
    , ecosystem_revenue as revenue
    , issuance * price as token_incentives
    , revenue - token_incentives as earnings
    
    -- Supply Metrics
    , issuance AS gross_emissions_native
    , issuance * price AS gross_emissions

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
left join voting_fees on fundamental_usage.date = voting_fees.date
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
where fundamental_usage.date < to_date(sysdate())
