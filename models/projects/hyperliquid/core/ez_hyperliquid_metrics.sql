{{
    config(
        materialized="table",
        snowflake_warehouse="HYPERLIQUID",
        database="hyperliquid",
        schema="core",
        alias="ez_metrics",
    )
}}

with trading_volume_data as (
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
    -- snapshot data only starts as early as 2024-01-06
    select date, chain, staked_hype, num_stakers
    from {{ ref("fact_hyperliquid_hype_staked") }}
)
, spot_trading_volume_data as (
    select date, spot_trading_volume, chain
    from {{ ref("fact_hyperliquid_spot_trading_volume") }}
)
, daily_supply_data as (
     select
         date,
         emissions_native,
         premine_unlocks_native,
     from {{ref('fact_hyperliquid_daily_supply_data')}}
 )
, perps_tvl_data as (
    select date, tvl
    from {{ ref("fact_hyperliquid_perps_tvl") }}
)
, market_metrics as (
    ({{ get_coingecko_metrics("hyperliquid") }}) 
)
, date_spine as (
    SELECT
        date,
        'hyperliquid' as chain
    FROM {{ref("dim_date_spine")}}
    WHERE date between '2023-06-13' and to_date(sysdate())
)
, hyperevm_fundamental_metrics_data as (
    select date, chain, daa, txns, hyperevm_burns, hyperevm_burns_native
    from {{ ref("fact_hyperliquid_hyperevm_fundamental_metrics") }}
)
    
select
    date_spine.date
    , 'hyperliquid' as app
    , 'DeFi' as category

    --Old metrics needed for compatibility
    , coalesce(perp_volume, 0) + coalesce(spot_trading_volume, 0) as trading_volume
    , unique_traders::string as unique_traders
    , trades as txns
    , trading_fees as fees
    , auction_fees
    , hypercore_burns_native + hyperevm_burns_native as daily_burns_native
    , trading_fees * 0.03 as primary_supply_side_revenue
    -- add daily burn back to the revenue
     , (daily_buybacks_native * mm.price) + (daily_burns_native * mm.price) as revenue
     , daily_buybacks_native
     , num_stakers
     , staked_hype

    -- Standardized Metrics

    -- Market Metrics
    , price
    , token_volume
    , market_cap
    , fdmc
    , token_turnover_circulating
    , token_turnover_fdv

    -- Usage Metrics
    , unique_traders::string + hyperevm_data.daa as perp_dau
    , perp_volume as perp_volume
    , trades + hyperevm_data.txns as perp_txns
    , perps_tvl_data.tvl as tvl
    
    -- Cash Flow Metrics
    , perp_fees
    , spot_fees
    -- all l1 fees are burned (HyperEVM) + Hypercore (Spot Token Fees Burned)
     , daily_burns_native * mm.price as chain_fees
     , trading_fees + (daily_burns_native * mm.price) as ecosystem_revenue
     , trading_fees * 0.03 as service_fee_allocation
     , (daily_buybacks_native * mm.price) as buyback_fee_allocation
     , daily_buybacks_native as buybacks_native
     , daily_burns_native as burned_fee_allocation_native
     , daily_burns_native * mm.price as burned_fee_allocation

    --HYPE Token Supply Data
    , coalesce(daily_supply_data.emissions_native, 0) as emissions_native
    , coalesce(daily_supply_data.premine_unlocks_native, 0) as premine_unlocks_native
    , coalesce(daily_burns_native, 0) as burns_native
    , coalesce(daily_supply_data.emissions_native, 0) + coalesce(daily_supply_data.premine_unlocks_native, 0) - coalesce(burns_native, 0) as net_supply_change_native
    , sum(coalesce(daily_supply_data.emissions_native, 0) + coalesce(daily_supply_data.premine_unlocks_native, 0) - coalesce(burns_native, 0)) over (order by daily_supply_data.date) as circulating_supply_native

from date_spine
left join market_metrics mm using(date)
left join unique_traders_data using(date)
left join trading_volume_data using(date)
left join daily_transactions_data using(date)
left join fees_data using(date)
left join hypercore_spot_burns_data using(date)
left join hyperevm_fundamental_metrics_data hyperevm_data using(date)
left join daily_supply_data using(date)
left join auction_fees_data using(date)
left join hype_staked_data using(date)
left join spot_trading_volume_data using(date)
left join daily_assistance_fund_data using(date)
left join perps_tvl_data using(date)
where date < to_date(sysdate())