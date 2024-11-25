{{
    config(
        materialized="table",
        snowflake_warehouse="metaplex",
        database="metaplex",
        schema="core",
        alias="ez_metrics",
    )
}}
-- 2020-10-25
with date_spine as (
    select date
    from {{ ref("dim_date_spine") }}
    where date between date('2021-03-29') and to_date(sysdate())
)
, revenue as (
    select
        date
        , sum(revenue_usd) as revenue_usd
    from {{ ref("fact_metaplex_revenue") }}
    group by 1
)
, buybacks as (
    select
        date
        , buyback
        , buyback_native
    from {{ ref("fact_metaplex_buybacks") }}
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
        , sum(daily_signed_transactions) as txns
    from {{ ref("fact_metaplex_transaction_counts") }}
    group by 1
)
, mints as (
    select
        date
        , daily_mints
        , cumulative_mints
    from {{ ref("fact_metaplex_assets_minted") }}
)
, price as (
    {{get_coingecko_metrics('metaplex')}}
)

SELECT
    ds.date
    , coalesce(revenue.revenue_usd, 0) as fees
    , coalesce(revenue.revenue_usd, 0) as revenue -- Fees + Revenue are same - 50% fees go to buybacks | the other 50% goes to dao treasury.
    , coalesce(buybacks.buyback, 0) as buyback -- 50% of fees (ie all of revenue) go to buybacks but buybacks are done in batches, at the time of the buyback
    , coalesce(buybacks.buyback_native, 0) as buyback_native
    , coalesce(mints.daily_mints, 0) as daily_mints
    , coalesce(mints.cumulative_mints, 0) as cumulative_mints
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
FROM date_spine ds
LEFT JOIN price USING (date)
LEFT JOIN revenue USING (date)
LEFT JOIN buybacks USING (date)
LEFT JOIN mints USING (date)
LEFT JOIN active_wallets USING (date)
LEFT JOIN transactions USING (date)
LEFT JOIN unique_signers USING (date)
LEFT JOIN new_holders USING (date)
where ds.date < to_date(sysdate())
