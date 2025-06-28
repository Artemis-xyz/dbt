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
    trading_volume_data as (
        select date, trading_volume as perp_volume, chain
        from {{ ref("fact_hyperliquid_trading_volume") }}
    )
    , unique_traders_data as (
        select date, unique_traders, chain
        from {{ ref("fact_hyperliquid_unique_traders") }}
    )
    , daily_transactions_data as (
        select date, trades, chain
        from {{ ref("fact_hyperliquid_daily_transactions") }}
    )
    , fees_data as (
        select date, chain, trading_fees, spot_fees, perp_fees
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
        ({{ get_coingecko_metrics("hyperliquid") }}) 
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
        select date, chain,new_users
        from {{ ref("fact_hyperliquid_new_users") }}
    )
select
    date
    , 'hyperliquid' as app
    , 'DeFi' as category
    , chain
    , spot_trading_volume
    , coalesce(perp_volume, 0) + coalesce(spot_trading_volume, 0) as trading_volume
    , unique_traders::string as unique_traders
    , trades as txns
    , auction_fees
    -- all l1 fees are burned (HyperEVM) + Hypercore (Spot Token Fees Burned)
    , coalesce(hypercore_burns_native, 0) + coalesce(hyperevm_burns_native, 0) as daily_burns_native
    , trading_fees * 0.03 as primary_supply_side_revenue
    -- add daily burn back to the revenue
    , (daily_buybacks_native * mm.price) + (daily_burns_native * mm.price) as revenue
    , daily_buybacks_native
    , num_stakers
    , staked_hype

    -- Standardized Metrics
    , unique_traders::string + hyperevm_data.daa as perp_dau
    , perp_volume
    , spot_trading_volume as spot_volume
    , trades + hyperevm_data.txns as perp_txns
    , new_users

    -- Revenue Metrics
    , perp_fees as perp_fees
    , spot_fees as spot_fees
    -- all l1 fees are burned (HyperEVM)
    , coalesce(hyperevm_burns_native, 0) * mm.price as chain_fees
    , trading_fees + (daily_burns_native * mm.price) as ecosystem_revenue

    -- all l1 fees are burned (HyperEVM) + Hypercore (Spot Token Fees Burned)
    , trading_fees + (daily_burns_native * mm.price) as fees
    , trading_fees * 0.03 as service_fee_allocation
    , (daily_buybacks_native * mm.price) as buyback_fee_allocation
    , daily_buybacks_native as buybacks_native
    , daily_burns_native as burned_fee_allocation_native
    , daily_burns_native * mm.price as burned_fee_allocation

    -- Market metrics
    , mm.price as price
    , mm.token_volume as token_volume
    , mm.market_cap as market_cap
    , mm.fdmc as fdmc
    , mm.token_turnover_circulating as token_turnover_circulating
from date_spine
left join unique_traders_data using(date, chain)
left join trading_volume_data using(date, chain)
left join daily_transactions_data using(date, chain)
left join fees_data using(date, chain)
left join auction_fees_data using(date, chain)
left join hypercore_spot_burns_data using(date, chain)
left join hyperevm_fundamental_metrics_data hyperevm_data using(date, chain)
left join daily_assistance_fund_data using(date, chain)
left join hype_staked_data using(date, chain)
left join spot_trading_volume_data using(date, chain)
left join market_metrics mm using(date)
left join new_users_data using(date, chain)
where date < to_date(sysdate())