{{
    config(
        materialized="table",
        snowflake_warehouse="ETHERFI",
        database="etherfi",
        schema="core",
        alias="ez_metrics",
    )
}}

with restaked_eth_metrics as (
    select
        date,
        chain,
        num_restaked_eth,
        amount_restaked_usd,
        num_restaked_eth_net_change,
        amount_restaked_usd_net_change
    from {{ ref('fact_etherfi_restaked_eth_count_with_usd_and_change') }}
)
, daily_supply_data as (
    select
        date,
        emissions_native,
        premine_unlocks_native,
        burns_native,
        net_supply_change_native,
        circulating_supply
    from {{ ref('fact_etherfi_daily_supply_data') }}
)
, liquidity_pool_fees as (
    select
        date,
        fees_usd
    from {{ ref('fact_etherfi_liquidity_pool_fees') }}
)
, auction_fees as (
    select
        date,
        fees_usd
    from {{ ref('fact_etherfi_auction_fees') }}
)
, defillama_tvl as (
    select
        date,
        stake_tvl,
        liquid_tvl,
        liquid_tvl * 0.000055 as liquid_fees_usd
    from {{ ref('fact_etherfi_tvl') }}
)
, date_spine as (
    select
        ds.date
    from {{ ref('dim_date_spine') }} ds
    where ds.date between (select min(date) from restaked_eth_metrics) and to_date(sysdate())
)
, market_metrics as (
    {{get_coingecko_metrics('ether-fi')}}
)

SELECT
    date_spine.date
    , 'etherfi' as app
    , 'DeFi' as category

    --Standardized Metrics

    --Market Metrics
    , market_metrics.price as price
    , market_metrics.token_volume as token_volume
    , market_metrics.market_cap as market_cap
    , market_metrics.fdmc as fdmc

    --Usage Metrics
    , restaked_eth_metrics.num_restaked_eth as tvl_native
    , restaked_eth_metrics.amount_restaked_usd as tvl
    , restaked_eth_metrics.num_restaked_eth_net_change as tvl_native_net_change
    , restaked_eth_metrics.amount_restaked_usd_net_change as tvl_net_change

    --Cash Flow Metrics
    , coalesce(liquidity_pool_fees.fees_usd, 0) as liquidity_pool_fees
    , coalesce(auction_fees.fees_usd, 0) as auction_fees
    , coalesce(defillama_tvl.liquid_fees_usd, 0) as strategy_fees
    , coalesce(liquidity_pool_fees.fees_usd, 0) + coalesce(auction_fees.fees_usd, 0) + coalesce(defillama_tvl.liquid_fees_usd, 0) as ecosystem_revenue
    , strategy_fees as equity_cash_flow

    --Token Turnover Metrics
    , market_metrics.token_turnover_circulating as token_turnover_circulating
    , market_metrics.token_turnover_fdv as token_turnover_fdv

    --ETHFI Token Supply Data
    , daily_supply_data.emissions_native
    , daily_supply_data.premine_unlocks_native
    , daily_supply_data.burns_native
    , daily_supply_data.net_supply_change_native
    , daily_supply_data.circulating_supply

from date_spine
left join restaked_eth_metrics using(date)
left join liquidity_pool_fees using(date)
left join auction_fees using(date)
left join defillama_tvl using(date)
left join daily_supply_data using(date)
left join market_metrics using(date)
where date < to_date(sysdate())

