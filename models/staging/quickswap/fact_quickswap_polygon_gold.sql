{{ config(materialized="table", snowflake_warehouse="DEX_SM") }}

with
    all_metrics as (
        select
            t1.date,
            t1.chain,
            'quickswap' as app,
            t1.category,
            coalesce(t1.tvl, 0) as tvl,
            coalesce(t2.unique_traders, 0) as unique_traders,
            coalesce(t3.trading_volume, 0) as trading_volume,
            coalesce(t3.fees, 0) as fees
        from {{ ref("fact_quickswap_tvl_polygon") }} t1
        full outer join
            {{ ref("fact_quickswap_unique_traders_polygon") }} t2 on t1.date = t2.date
        full outer join
            {{ ref("fact_quickswap_trading_vol_fees_polygon") }} t3 on t1.date = t3.date
    )
select date, chain, app, category, tvl, unique_traders, trading_volume, fees
from all_metrics
