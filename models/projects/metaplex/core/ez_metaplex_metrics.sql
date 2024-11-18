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
, unique_signers as (
    select
        date
        , sum(unique_signers) as unique_signers
    from {{ ref("fact_metaplex_unique_signers") }}
    group by 1
)
, new_holders as (
    select
        date
        , sum(daily_new_holders) as daily_new_holders
    from {{ ref("fact_metaplex_new_holders") }}
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
    , coalesce(revenue.revenue_usd * 2, 0) as fees
    , coalesce(revenue.revenue_usd, 0) as revenue -- Fees are paid continuously, but revenue is only recognized at the time of the buyback
    , coalesce(buybacks.buyback, 0) as buyback -- 50% of fees (ie all of revenue) go to buybacks but buybacks are done in batches, at the time of the buyback
    , coalesce(buybacks.buyback_native, 0) as buyback_native
    , coalesce(mints.daily_mints, 0) as daily_mints
    , coalesce(active_wallets.dau, 0) as dau
    , coalesce(transactions.txns, 0) as txns
    , coalesce(unique_signers.unique_signers, 0) as unique_signers
    , coalesce(new_holders.daily_new_holders, 0) as daily_new_holders
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
LEFT JOIN unique_signers USING (date)
LEFT JOIN new_holders USING (date)
where coalesce(price.date, revenue.date, buybacks.date, transactions.date) < to_date(sysdate())
