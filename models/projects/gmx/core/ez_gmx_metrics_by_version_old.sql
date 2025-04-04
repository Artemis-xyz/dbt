{{
    config(
        materialized="table",
        snowflake_warehouse="GMX",
        database="gmx",
        schema="core",
        alias="ez_metrics_by_version_old",
    )
}}

with
    trading_volume_data as (
        select date, trading_volume
        from {{ ref("fact_gmx_trading_volume") }}
        where chain is null
    ),
    unique_traders_data as (
        select date, unique_traders
        from {{ ref("fact_gmx_unique_traders") }}
        where chain is null
    ),
    v1_data as (
        select 
            t1.date, 
            trading_volume, 
            unique_traders, 
            'GMX v1' as version
        from  trading_volume_data t1
        left join unique_traders_data t2 on t1.date = t2.date
    ),
    v2_data as (
        select 
            date, 
            trading_volume, 
            unique_traders, 
            'GMX v2' as version
        from {{ ref("fact_gmx_v2_trading_volume_unique_traders") }}
        where chain is null
    )
   
select 
    date,
    'gmx' as app,
    'DeFi' as category,
    version,
    trading_volume as spot_volume,
    unique_traders as spot_dau,
    tvl_usd as tvl
from v2_data
where date < to_date(sysdate())
union all
select 
    date ,
    'gmx' as app,
    'DeFi' as category,
    version,
    trading_volume as spot_volume,
    unique_traders as spot_dau,
    tvl_usd as tvl
from v1_data
where date < to_date(sysdate())
