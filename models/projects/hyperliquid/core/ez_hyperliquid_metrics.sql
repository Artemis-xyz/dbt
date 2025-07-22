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
, first_principles_supply_data as (
    select
        date,
        emissions_native,
        premine_unlocks_native,
    from {{ref('fact_hyperliquid_daily_supply_data')}}
)
, hyperliquid_api_supply_data as (
    select 
        date
        , max_supply_native
        , uncreated_tokens
        , total_supply_native
        , burn_tokens
        , foundation_owned_balances
        , issued_supply_native
        , unvested_tokens
        , net_supply_change_native
        , circulating_supply_native
    from {{ref('fact_hyperliquid_supply_data')}}
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
    WHERE date between '2024-11-29' and to_date(sysdate())
)
, hyperevm_fundamental_metrics_data as (
    select date, chain, daa, txns, hyperevm_burns, hyperevm_burns_native
    from {{ ref("fact_hyperliquid_hyperevm_fundamental_metrics") }}
)

, chain_tvl as (
    with agg as (
        SELECT 
            date
            , tvl 
        FROM {{ref('fact_defillama_chain_tvls')}}
        WHERE defillama_chain_name ilike '%hype%'
        UNION ALL
        SELECT 
            t.date
            , -sum(tvl) as tvl 
        FROM {{ref('fact_defillama_protocol_tvls')}} t
        JOIN {{ref('fact_defillama_protocols')}} p ON p.id = t.defillama_protocol_id
        WHERE name in (
            'Hyperliquid HLP'
        , 'Hyperliquid Spot Orderbook'
        )
        GROUP BY 1
    )
    SELECT 
        date
        , sum(tvl) as tvl 
    FROM agg
    WHERE date > '2025-02-24'
    GROUP BY 1
    )

, new_users_data as (
    select date, new_users
    from {{ ref("fact_hyperliquid_new_users") }}
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
    -- all l1 fees are burned (HyperEVM) + Hypercore (Spot Token Fees Burned)
    , coalesce(hypercore_burns_native, 0) + coalesce(hyperevm_burns_native, 0) as daily_burns_native
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
    , spot_trading_volume as spot_volume
    , trades + hyperevm_data.txns as perp_txns
    , chain_tvl.tvl as chain_tvl
    , coalesce(perps_tvl_data.tvl, 0) as tvl
    , new_users
    
    -- Cash Flow Metrics
    , perp_fees
    , spot_fees
    -- all l1 fees are burned (HyperEVM)
     , coalesce(hyperevm_burns_native, 0) * mm.price as chain_fees
     , trading_fees + (daily_burns_native * mm.price) as ecosystem_revenue
     , trading_fees * 0.03 as service_fee_allocation
     , (daily_buybacks_native * mm.price) as buyback_fee_allocation
     , daily_buybacks_native as buyback_native
     , daily_burns_native as burned_fee_allocation_native
     , daily_burns_native * mm.price as burned_fee_allocation

    --HYPE Token Supply Data
    , coalesce(emissions_native, 0) as emissions_native
    , coalesce(premine_unlocks_native, 0) as premine_unlocks_native
    , coalesce(daily_burns_native, 0) as burns_native
    , coalesce(hyperliquid_api_supply_data.max_supply_native, 0) as max_supply_native
    , coalesce(hyperliquid_api_supply_data.total_supply_native, 0) as total_supply_native
    , coalesce(hyperliquid_api_supply_data.issued_supply_native, 0) as issued_supply_native
    , coalesce(hyperliquid_api_supply_data.net_supply_change_native, 0) as net_supply_change_native
    , coalesce(hyperliquid_api_supply_data.circulating_supply_native, 0) as circulating_supply_native
    --, sum(coalesce(daily_supply_data.emissions_native, 0) + coalesce(daily_supply_data.premine_unlocks_native, 0) - coalesce(burns_native, 0)) over (order by daily_supply_data.date) as circulating_supply_native

from date_spine
left join market_metrics mm using(date)
left join unique_traders_data using(date)
left join trading_volume_data using(date)
left join daily_transactions_data using(date)
left join fees_data using(date)
left join hypercore_spot_burns_data using(date)
left join hyperevm_fundamental_metrics_data hyperevm_data using(date)
left join first_principles_supply_data using(date)
left join hyperliquid_api_supply_data using(date)
left join auction_fees_data using(date)
left join hype_staked_data using(date)
left join spot_trading_volume_data using(date)
left join daily_assistance_fund_data using(date)
left join perps_tvl_data using(date)
left join chain_tvl using(date)
left join new_users_data using(date)
where date_spine.date < to_date(sysdate())