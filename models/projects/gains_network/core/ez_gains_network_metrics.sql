{{
    config(
        materialized="incremental",
        snowflake_warehouse="GAINS_NETWORK",
        database="gains_network",
        schema="core",
        alias="ez_metrics",
        incremental_strategy="merge",
        unique_key="date",
        on_schema_change="append_new_columns",
        merge_update_columns=var("backfill_columns", []),
        merge_exclude_columns=["created_on"] if not var("backfill_columns", []) else none,
        full_refresh=false,
        tags=["ez_metrics"],
    )
}}

-- https://gains-network.gitbook.io/docs-home/liquidity-farming-pools/gns-staking
-- 55% of revenue to stakers before
-- Post Jul 12, 2024 this shifted to 60% and of that 60% 90% goes to buyback and burn. 10% to treasury
-- rest of the fees goes to 

{% set backfill_date = var("backfill_date", None) %}

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
            , treasury_fee_allocation
            , case when date <= '2024-07-12' then (gns_stakers + dai_stakers) else dai_stakers end as staking_fee_allocation
            , case when date > '2024-07-12' then gns_stakers else 0 end as buybacks
            , foundation_fee_allocation
            , service_fee_allocation
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
    , 'gains-network' as artemis_id
    -- Standardized Metrics

    -- Usage Metrics
    , gd.trading_volume as perp_volume
    , gd.unique_traders as perp_dau
    , gt.tvl

    -- Fee Metrics
    , gf.referral_fees
    , gf.nft_bot_fees
    , gf.fees
    , gf.buybacks as buyback_fee_allocation
    , gf.foundation_fee_allocation
    , gf.staking_fee_allocation
    , gf.service_fee_allocation
    , gf.treasury_fee_allocation
    
    -- Financial Metrics
    , gf.revenue

    -- timestamp columns
    , sysdate() as created_on
    , sysdate() as modified_on
from date_spine ds
left join gains_data gd using (date)
left join gains_fees gf using (date)
left join gains_tvl gt using (date)
where true
{{ ez_metrics_incremental('ds.date', backfill_date) }}
and ds.date < to_date(sysdate())
