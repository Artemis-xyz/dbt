-- depends_on {{ ref("fact_tron_transactions_v2") }}
{{
    config(
        materialized="table",
        snowflake_warehouse="BAM_TRANSACTION_XLG",
        database="tron",
        schema="core",
        alias="ez_metrics",
    )
}}

with fundamental_data as (
    {{ get_fundamental_data_for_chain("tron", "v2") }}
)
,    market_metrics as ({{ get_coingecko_metrics("tron") }})
,    defillama_data as ({{ get_defillama_metrics("tron") }})
,    stablecoin_data as ({{ get_stablecoin_metrics("tron") }})
,    github_data as ({{ get_github_metrics("tron") }})
,    p2p_metrics as ({{ get_p2p_metrics("tron") }})
,    rolling_metrics as ({{ get_rolling_active_address_metrics("tron") }})
,    token_incentives as (
    select 
        date,
        sum(token_incentives) as token_incentives,
    from {{ ref("fact_tron_token_incentives") }}
    group by date
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
    , fees
    , fees AS revenue
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
    , fees_native AS ecosystem_revenue_native
    , fees AS ecosystem_revenue
    , fees_native AS burned_cash_flow_native
    , fees AS burned_cash_flow

    -- Financial Statement Metrics
    , fees as fees
    , burned_cash_flow as revenue
    , token_incentives.token_incentives as token_incentives
    , revenue - token_incentives as earnings

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


from fundamental_data
left join market_metrics on fundamental_data.date = market_metrics.date
left join defillama_data on fundamental_data.date = defillama_data.date
left join stablecoin_data on fundamental_data.date = stablecoin_data.date
left join github_data on fundamental_data.date = github_data.date
left join p2p_metrics on fundamental_data.date = p2p_metrics.date
left join rolling_metrics on fundamental_data.date = rolling_metrics.date
left join token_incentives on fundamental_data.date = token_incentives.date
where fundamental_data.date < to_date(sysdate())
