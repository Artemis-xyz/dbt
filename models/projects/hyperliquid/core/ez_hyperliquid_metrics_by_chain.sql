{{
    config(
        materialized="table",
        snowflake_warehouse="HYPERLIQUID",
        database="hyperliquid",
        schema="core",
        alias="ez_metrics_by_chain",
    )
}}

with
    perp_volume_data as (
        select date, coalesce(trading_volume, 0) as perp_volume, chain
        from {{ ref("fact_hyperliquid_trading_volume") }}
    )
    , unique_traders_data as (
        select date, coalesce(unique_traders, 0) as unique_traders, chain
        from {{ ref("fact_hyperliquid_unique_traders") }}
    )
    , daily_transactions_data as (
        select date, coalesce(trades, 0) as trades, chain
        from {{ ref("fact_hyperliquid_daily_transactions") }}
    )
    , fees_data as (
        select date, chain, coalesce(trading_fees, 0) as trading_fees, coalesce(spot_fees, 0) as spot_fees, coalesce(perp_fees, 0) as perp_fees
        from {{ ref("fact_hyperliquid_fees") }}
    )
    , auction_fees_data as (
        select date, chain, sum(auction_fees) as auction_fees
        from {{ ref("fact_hyperliquid_auction_fees") }}
        group by 1, 2
    )
    , hypercore_spot_burns_data as (
        select date, hypercore_burns_native, chain
        from {{ ref("fact_hyperliquid_hypercore_burns") }}
    )
    , daily_assistance_fund_data as (
        select date, daily_balance as daily_buybacks_native, balance as assistance_fund_balance, chain
        from {{ ref("fact_hyperliquid_assistance_fund_balance") }}
    )
    , hype_staked_data as (
        select date, chain, staked_hype, num_stakers
        from {{ ref("fact_hyperliquid_hype_staked") }}
    )
    , spot_trading_volume_data as (
        select date, spot_trading_volume, chain
        from {{ ref("fact_hyperliquid_spot_trading_volume") }}
    )
    , market_metrics as (
        {{ get_coingecko_metrics("hyperliquid") }}
    )
    , date_spine as (
        SELECT
            date,
            'hyperliquid' as chain
        FROM {{ref("dim_date_spine")}}
        -- start date of Hyperliquid
        WHERE date between '2024-11-29' and to_date(sysdate())
    )
    , hyperevm_fundamental_metrics_data as (
        select date, chain, daa, txns, hyperevm_burns, hyperevm_burns_native
        from {{ ref("fact_hyperliquid_hyperevm_fundamental_metrics") }}
    )
    , new_users_data as (
        select date, chain, new_users
        from {{ ref("fact_hyperliquid_new_users") }}
    )
    , market_metrics as (
        {{ get_coingecko_metrics("hyperliquid") }}
    )
select
    date_spine.date
    , 'hyperliquid' as artemis_id
    , 'hyperliquid' as chain
    
    -- Standardized Metrics

    -- Usage Data
    , coalesce(unique_traders_data.unique_traders, 0)::string + coalesce(hyperevm_data.daa, 0) as perp_dau
    , coalesce(unique_traders_data.unique_traders, 0)::string + coalesce(hyperevm_data.daa, 0) as dau
    , daily_transactions_data.trades as perp_txns
    , daily_transactions_data.trades as txns
    , perp_volume_data.perp_volume
    , spot_trading_volume_data.spot_trading_volume as spot_volume
    , coalesce(perp_volume_data.perp_volume, 0) + coalesce(spot_trading_volume_data.spot_trading_volume, 0) as volume
    , hype_staked_data.num_stakers
    , hype_staked_data.staked_hype as total_staked_native
    , hype_staked_data.staked_hype * market_metrics.price as total_staked
    
    -- Fee Data
    , fees_data.perp_fees
    , fees_data.spot_fees
    , auction_fees_data.auction_fees
    , hyperevm_data.hyperevm_burns_native * market_metrics.price as chain_fees -- A portion of HyperEVM fees are burned
    , coalesce(fees_data.trading_fees, 0) + coalesce(chain_fees, 0) as fees -- trading fees = (spot + perp) + auction fees
    , fees_data.trading_fees * 0.03 as service_fee_allocation
    , (daily_assistance_fund_data.daily_buybacks_native * market_metrics.price) as buyback_fee_allocation -- 97% of trading fees are bought back to the Assistance Fund

    -- Financial Statements
    , daily_assistance_fund_data.daily_buybacks_native as buybacks_native
    , daily_assistance_fund_data.daily_buybacks_native * market_metrics.price as buybacks
    , daily_assistance_fund_data.daily_buybacks_native * market_metrics.price + coalesce(hypercore_spot_burns_data.hypercore_burns_native, 0) + coalesce(hyperevm_data.hyperevm_burns_native, 0) * market_metrics.price as revenue -- burns + buybacks

    -- Supply Data
    , coalesce(hypercore_spot_burns_data.hypercore_burns_native, 0) + coalesce(hyperevm_data.hyperevm_burns_native, 0) as burns_native

from date_spine
left join unique_traders_data using(date, chain)
left join perp_volume_data using(date, chain)
left join daily_transactions_data using(date, chain)
left join fees_data using(date, chain)
left join auction_fees_data using(date, chain)
left join hypercore_spot_burns_data using(date, chain)
left join hyperevm_fundamental_metrics_data hyperevm_data using(date, chain)
left join daily_assistance_fund_data using(date, chain)
left join hype_staked_data using(date, chain)
left join spot_trading_volume_data using(date, chain)
left join market_metrics using(date)
left join new_users_data using(date, chain)
where date_spine.date < to_date(sysdate())