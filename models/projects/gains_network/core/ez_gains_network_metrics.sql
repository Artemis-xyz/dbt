{{
    config(
        materialized="table",
        snowflake_warehouse="GAINS_NETWORK",
        database="gains_network",
        schema="core",
        alias="ez_metrics",
    )
}}

-- https://gains-network.gitbook.io/docs-home/liquidity-farming-pools/gns-staking
-- 55% of revenue to stakers before
-- Post Jul 12, 2024 this shifted to 60% and of that 60% 90% goes to buyback and burn. 10% to treasury
-- rest of the fees goes to 

with date_spine as (
    select date
    from {{ ref("dim_date_spine") }}
    where date between '2022-12-14' and to_date(sysdate())
)

    , gains_data as (
        with agg as (
            select date, sum(trading_volume) as trading_volume, sum(unique_traders) as unique_traders
            from {{ ref("fact_gains_trading_volume_unique_traders") }} -- V7
            where chain is not null
            group by date
            UNION ALL
            SELECT date, sum(trading_volume) as trading_volume, sum(unique_traders) as unique_traders
            from {{ ref("fact_gains_data_v8_v9") }} -- V8 and V9
            group by date
        )
        SELECT
            date
            , sum(trading_volume) as trading_volume
            , sum(unique_traders) as unique_traders
        FROM agg
        group by date
    )
    , gains_fees as (
        select 
            date
            , fees
            , revenue
            , treasury_cash_flow
            , case when date <= '2024-07-12' then (gns_stakers + dai_stakers) else dai_stakers end as staking_cash_flow
            , case when date > '2024-07-12' then gns_stakers else 0 end as buybacks
            , foundation_cash_flow
            , service_cash_flow
            , referral_fees
            , nft_bot_fees
        from {{ ref("fact_gains_fees") }}
    )
    , gains_tvl as (
        select date, sum(usd_balance) as tvl
        from {{ ref("fact_gains_tvl") }}
        group by date
    )

select
    ds.date
    , 'gains-network' as app
    , 'DeFi' as category
    , gd.trading_volume
    , gd.unique_traders
    , gf.fees
    , gf.revenue
    , gt.tvl
    -- standardize metrics
    , gd.trading_volume as perp_volume
    , gd.unique_traders as perp_dau
    , gf.referral_fees
    , gf.nft_bot_fees
    , gf.fees as ecosystem_revenue
    , gf.buybacks as buyback_cash_flow
    , gf.foundation_cash_flow
    , gf.staking_cash_flow
    , gf.service_cash_flow
    , gf.treasury_cash_flow
from date_spine ds
left join gains_data gd using (date)
left join gains_fees gf using (date)
left join gains_tvl gt using (date)
