{{
    config(
        materialized="table",
        snowflake_warehouse="metaplex",
        database="metaplex",
        schema="core",
        alias="ez_metrics",
    )
}}

with revenue as (
    select
        date
        , sum(revenue_usd) as revenue_usd
    from {{ ref("fact_metaplex_revenue") }}
    group by 1
)
, buybacks as (
    select
        date
        , sum(buyback_usd) * -1 as buyback
        , sum(buyback_native) * -1 as buyback_native
    from {{ ref("fact_metaplex_buybacks") }}
    group by 1
)
, active_wallets as (
    select
        date
        , sum(daily_active_users) as dau
    from {{ ref("fact_metaplex_active_wallets") }}
    group by 1
)
, transactions as (
    select
        date
        , sum(txns) as txns
    from {{ ref("fact_metaplex_transaction_counts") }}
    group by 1
)
, mints as (
    select
        date
        , sum(daily_mints) as daily_mints
    from {{ ref("fact_metaplex_assets_minted") }}
    group by 1
)
, price as (
    {{get_coingecko_metrics('metaplex')}}
)

SELECT
    coalesce(price.date, revenue.date, buybacks.date, transactions.date) as date
    , coalesce(revenue.revenue_usd, 0) as fees
    , coalesce(revenue.revenue_usd, 0) as revenue -- 50% of fees go to buybacks but buybacks are done in batches, while fees are paid continuously
    , coalesce(buybacks.buyback, 0) as buyback -- buybacks in USD, at the time of the buyback
    , coalesce(buybacks.buyback_native, 0) as buyback_native
    , coalesce(mints.daily_mints, 0) as daily_mints
    , coalesce(active_wallets.dau, 0) as dau
    , coalesce(transactions.txns, 0) as txns
    , coalesce(price.price, 0) as price
    , coalesce(price.market_cap, 0) as market_cap
    , price.fdmc
    , price.token_turnover_circulating
    , price.token_turnover_fdv
    , price.token_volume
FROM price
LEFT JOIN revenue USING (date)
LEFT JOIN buybacks USING (date)
LEFT JOIN mints USING (date)
LEFT JOIN active_wallets USING (date)
LEFT JOIN transactions USING (date)
where coalesce(price.date, revenue.date, buybacks.date, transactions.date) < to_date(sysdate())
