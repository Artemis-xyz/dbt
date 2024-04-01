{{ config(materialized="table", snowflake_warehouse="PANCAKESWAP_SM") }}
with
    chain_tvl as (
        select date, chain, 'pancakeswap' as app, category, tvl
        from {{ ref("fact_pancakeswap_v2_tvl_bsc") }}
        union all
        select date, chain, 'pancakeswap' as app, category, tvl
        from {{ ref("fact_pancakeswap_v3_tvl_bsc") }}
    ),
    combined_tvl as (
        select date, chain, app, category, sum(tvl) as tvl
        from chain_tvl
        group by date, chain, app, category
    ),
    chain_unique_traders as (
        select date, chain, 'pancakeswap' as app, category, unique_traders
        from {{ ref("fact_pancakeswap_v2_unique_traders_bsc") }}
        union all
        select date, chain, 'pancakeswap' as app, category, unique_traders
        from {{ ref("fact_pancakeswap_v3_unique_traders_bsc") }}
    ),
    combined_unique_traders as (
        select date, chain, app, category, sum(unique_traders) as unique_traders
        from chain_unique_traders
        group by date, chain, app, category
    ),
    chain_vol_fees as (
        select date, chain, 'pancakeswap' as app, category, trading_volume, fees
        from {{ ref("fact_pancakeswap_v2_trading_vol_and_fees_bsc") }}
        union all
        select date, chain, 'pancakeswap' as app, category, trading_volume, fees
        from {{ ref("fact_pancakeswap_v3_trading_vol_and_fees_bsc") }}
    ),
    combined_vol_fees as (
        select
            date,
            chain,
            app,
            category,
            sum(trading_volume) as trading_volume,
            sum(fees) as fees
        from chain_vol_fees
        group by date, chain, app, category
    ),
    all_metrics as (
        select
            t1.date,
            t1.chain,
            'pancakeswap' as app,
            t1.category,
            coalesce(t1.tvl, 0) as tvl,
            coalesce(t2.unique_traders, 0) as unique_traders,
            coalesce(t3.trading_volume, 0) as trading_volume,
            coalesce(t3.fees, 0) as fees
        from combined_tvl t1
        full outer join combined_unique_traders t2 on t1.date = t2.date
        full outer join combined_vol_fees t3 on t1.date = t3.date
    )
select date, chain, app, category, tvl, unique_traders, trading_volume, fees
from all_metrics
