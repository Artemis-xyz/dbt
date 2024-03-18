{{ config(materialized="table", snowflake_warehouse="UNISWAP_SM") }}

with
    uniswap_chain_tvl as (
        select date, chain, 'uniswap' as app, category, tvl
        from {{ ref("fact_uniswap_v2_tvl_ethereum") }}
        union all
        select date, chain, 'uniswap' as app, category, tvl
        from {{ ref("fact_uniswap_v3_tvl_ethereum") }}
    ),
    combined_uniswap_tvl as (
        select date, chain, app, category, sum(tvl) as tvl
        from uniswap_chain_tvl
        group by date, chain, app, category
    ),
    uniswap_chain_unique_traders as (
        select date, chain, 'uniswap' as app, category, unique_traders
        from {{ ref("fact_uniswap_v2_unique_traders_ethereum") }}
        union all
        select date, chain, 'uniswap' as app, category, unique_traders
        from {{ ref("fact_uniswap_v3_unique_traders_ethereum") }}
    ),
    combined_uniswap_unique_traders as (
        select date, chain, app, category, sum(unique_traders) as unique_traders
        from uniswap_chain_unique_traders
        group by date, chain, app, category
    ),
    uniswap_chain_vol_fees as (
        select date, chain, 'uniswap' as app, category, trading_volume, fees
        from {{ ref("fact_uniswap_v2_trading_vol_and_fees_ethereum") }}
        union all
        select date, chain, 'uniswap' as app, category, trading_volume, fees
        from {{ ref("fact_uniswap_v3_trading_vol_and_fees_ethereum") }}
    ),
    combined_uniswap_vol_fees as (
        select
            date,
            chain,
            app,
            category,
            sum(trading_volume) as trading_volume,
            sum(fees) as fees
        from uniswap_chain_vol_fees
        group by date, chain, app, category
    ),
    all_metrics as (
        select
            t1.date,
            t1.chain,
            'uniswap' as app,
            t1.category,
            coalesce(t1.tvl, 0) as tvl,
            coalesce(t2.unique_traders, 0) as unique_traders,
            coalesce(t3.trading_volume, 0) as trading_volume,
            coalesce(t3.fees, 0) as fees
        from combined_uniswap_tvl t1
        full outer join combined_uniswap_unique_traders t2 on t1.date = t2.date
        full outer join combined_uniswap_vol_fees t3 on t1.date = t3.date
    )
select date, chain, app, category, tvl, unique_traders, trading_volume, fees
from all_metrics
