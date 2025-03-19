{{
    config(
        materialized="table",
        snowflake_warehouse="SYNTHETIX",
        database="synthetix",
        schema="core",
        alias="ez_metrics",
    )
}}

with
    trading_volume_data as (
        select date, trading_volume
        from {{ ref("fact_synthetix_trading_volume") }}
    ),
    unique_traders_data as (
        select date, unique_traders
        from {{ ref("fact_synthetix_unique_traders") }}
    ), 
    tvl as (
        select 
            date,
            sum(tvl_usd) as tvl_usd,
        from {{ ref("fact_synthetix_tvl_by_chain_and_token") }}
        group by 1 
    ),
    net_deposits as (
        select
            date,
            sum(net_deposits) as net_deposits
        from {{ ref("fact_synthetix_net_deposits_by_chain") }}
        group by 1
    ), 
    token_holders as (
        select
            date,
            token_holder_count
        from {{ ref('fact_synthetix_token_holders') }}
    ),
    fees as (
        select
            date,
            daily_fees as fees
        from {{ ref('fact_synthetix_fees') }}
    ),
    expenses as (
        select
            date,
            daily_expenses as expenses
        from {{ ref('fact_synthetix_expenses') }}
    ),
    market_data as (
        {{ get_coingecko_metrics('synthetix') }}
    )
select
    date,
    'synthetix' as app,
    'DeFi' as category,
    trading_volume,
    unique_traders,
    tvl_usd,
    net_deposits,
    fees,
    fees as revenue, 
    expenses,
    coalesce(market_data.price, 0) as price,
    coalesce(market_data.market_cap, 0) as market_cap,
    coalesce(market_data.fdmc, 0) as fdmc,
    coalesce(market_data.token_turnover_circulating, 0) as token_turnover_circulating,
    coalesce(market_data.token_turnover_fdv, 0) as token_turnover_fdv,
    coalesce(market_data.token_volume, 0) as token_volume,
    coalesce(token_holders.token_holder_count, 0) as token_holder_count
from unique_traders_data
left join trading_volume_data using(date)
left join tvl using(date)
left join net_deposits using(date)
left join fees using(date)
left join expenses using(date)
left join market_data using(date)
left join token_holders using(date)
where date < to_date(sysdate())