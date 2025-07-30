{{
    config(
        materialized="table",
        snowflake_warehouse="metaplex",
        database="metaplex",
        schema="core",
        alias="ez_metrics_by_chain",
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
        , coalesce(sum(revenue_usd), 0) as revenue_usd
    from {{ ref("fact_metaplex_revenue") }}
    group by 1
)
, buybacks as (
    select
        date
        , coalesce(buyback, 0) as buyback
        , coalesce(buyback_native, 0) as buyback_native
    from {{ ref("fact_metaplex_buybacks") }}
)
, active_wallets as (
    select
        date
        , coalesce(sum(daily_active_users), 0) as dau
    from {{ ref("fact_metaplex_active_wallets") }}
    group by 1
)
, unique_signers as (
    select
        date
        , coalesce(sum(unique_signers), 0) as unique_signers
    from {{ ref("fact_metaplex_unique_signers") }}
    group by 1
)
, new_holders as (
    select
        date
        , coalesce(sum(daily_new_holders), 0) as daily_new_holders
    from {{ ref("fact_metaplex_new_holders") }}
    group by 1
)
, transactions as (
    select
        date
        , coalesce(sum(daily_signed_transactions), 0) as txns
    from {{ ref("fact_metaplex_transaction_counts") }}
    group by 1
)
, mints as (
    select
        date
        , coalesce(daily_mints, 0) as daily_mints
        , coalesce(cumulative_mints, 0) as cumulative_mints
    from {{ ref("fact_metaplex_assets_minted") }}
)

SELECT
    date_spine.date
    , 'solana' as chain
    , 'metaplex' as artemis_id

    -- Standardized Metrics

    -- Usage Data
    , active_wallets.dau as nft_dau
    , active_wallets.dau as dau
    , transactions.txns as nft_txns
    , transactions.txns as txns
    , unique_signers.unique_signers as unique_signers
    , new_holders.daily_new_holders as daily_new_holders

    -- Fee Data
    , revenue.revenue_usd as nft_fees
    , 0.5 * revenue.revenue_usd as treasury_fee_allocation
    , 0.5 * revenue.revenue_usd as buyback_fee_allocation
    , buybacks.buyback as buybacks -- 50% of fees (ie all of revenue) go to buybacks but buybacks are done in batches, at the time of the buyback

    -- Financial Statements
    , revenue.revenue_usd as revenue -- Fees & Revenue are same - 50% fees go to buybacks | the other 50% goes to dao treasury.

    -- Supply Data
    , mints.daily_mints as gross_emissions_native
    , supply.premine_unlocks_native
    , supply.net_supply_change_native
    , supply.circulating_supply_native

FROM date_spine
LEFT JOIN revenue USING (date)
LEFT JOIN buybacks USING (date)
LEFT JOIN mints USING (date)
LEFT JOIN active_wallets USING (date)
LEFT JOIN transactions USING (date)
LEFT JOIN unique_signers USING (date)
LEFT JOIN new_holders USING (date)
where date_spine.date < to_date(sysdate())
