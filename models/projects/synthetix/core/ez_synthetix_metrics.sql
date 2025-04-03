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
            sum(tvl_usd) as tvl,
        from {{ ref("fact_synthetix_tvl_by_chain_and_token") }}
        group by 1 
    ),
    token_holders as (
        select
            date,
            sum(token_holder_count) as token_holder_count
        from {{ ref('fact_synthetix_token_holders') }}
        group by 1
    ),
    fees as (
        select
            date,
            fees as fees
        from {{ ref('fact_synthetix_fees') }}
    ),
    expenses as (
        select
            date,
            daily_expenses as expenses
        from {{ ref('fact_synthetix_expenses') }}
    ),
    token_incentives as (
        select
            date,
            sum(token_incentives) as token_incentives
        from {{ ref("fact_synthetix_token_incentives_by_chain") }}
        group by 1
    ),
    treasury_value as (
        select
            date,
            sum(usd_balance) as treasury_value
        from {{ ref('fact_synthetix_treasury_by_token') }}
        group by 1
    ),
    treasury_native as (
        SELECT
            date,
            sum(native_balance) as treasury_value_native
        FROM {{ ref('fact_synthetix_treasury_by_token') }}
        where token = 'SNX'
        group by 1
    ),
    net_treasury as (
        SELECT
            date,
            sum(usd_balance) as net_treasury_value
        FROM {{ ref('fact_synthetix_treasury_by_token') }}
        where token <> 'SNX'
        group by 1
    ),
    market_data as (
        {{ get_coingecko_metrics('havven') }}
    )
select
    date,
    'synthetix' as app,
    'DeFi' as category,
    coalesce(trading_volume, 0) as trading_volume,
    coalesce(unique_traders, 0) as unique_traders,
    coalesce(tvl, 0) as tvl,
    coalesce(tvl, 0) as net_deposits,
    coalesce(fees, 0) as fees,
    coalesce(fees, 0) as revenue, 
    coalesce(expenses, 0) as operating_expenses,
    coalesce(expenses + token_incentives, 0) as expenses,
    coalesce(revenue, 0) - coalesce(expenses,0) - coalesce(token_incentives, 0) as earnings,
    coalesce(token_incentives, 0) as token_incentives,
    coalesce(treasury_value, 0) as treasury_value,
    coalesce(treasury_value_native, 0) as treasury_value_native,
    coalesce(net_treasury_value, 0) as net_treasury_value,
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
left join fees using(date)
left join expenses using(date)
left join token_incentives using(date)
left join treasury_value using(date)
left join treasury_native using(date)
left join net_treasury using(date)
left join market_data using(date)
left join token_holders using(date)
where date < to_date(sysdate())