{{
    config(
        materialized="table",
        snowflake_warehouse="GMX",
        database="gmx",
        schema="core",
        alias="ez_metrics",
    )
}}

with
    gmx_dex_swaps_v1 as (
        select 
            date,
            count(distinct sender) as spot_dau,
            sum(amountOut_usd) as spot_volume,
            sum(amount_fees_usd) as spot_fees,
            sum(amount_fees_usd) as spot_revenue
        from {{ ref("fact_gmx_v1_dex_swaps") }}
        group by 1
    ),
    gmx_dex_swaps_v2 as (
        select 
            date,
            count(distinct sender) as spot_dau,
            sum(amountOut_usd) as spot_volume,
            sum(amount_fees_usd) as spot_fees,
            sum(amount_fees_usd) as spot_revenue
        from {{ ref("fact_gmx_v2_dex_swaps") }}
        group by 1
    ),
    treasury_metrics as (
        select * from {{ ref("fact_gmx_treasury_data") }}
    ),
    tvl_metrics as (
        select
            date,
            sum(tvl_token_adjusted) as tvl
        from {{ ref("fact_gmx_all_versions_tvl") }}
        where version = 'v2'
        group by 1

        union all   

        select
            date,
            sum(tvl_token_adjusted) as tvl
        from {{ ref("fact_gmx_all_versions_tvl") }}
        where version = 'v1'
        group by 1
    ),
    tvl_metrics_grouped as (
        select
            date,
            sum(tvl) as tvl
        from tvl_metrics
        group by 1
    ),
    date_spine as (
        select date
        from {{ ref("dim_date_spine") }}
        where date between '2020-03-01' and to_date(sysdate())
    ),
    treasury as (
        select
            date,
            sum(usd_balance) as net_treasury_usd
        from {{ ref('fact_gmx_treasury_data') }}
        group by 1
    ), treasury_native as (
        select
            date,
            sum(native_balance) as treasury_native
        from {{ ref('fact_gmx_treasury_data') }}
        where token = 'GMX'
        group by 1
    ), net_treasury as (
        select
            date,
            sum(usd_balance) as net_treasury_usd
        from {{ ref('fact_gmx_treasury_data') }}
        where token <> 'GMX'
        group by 1
    ),
    market_metrics as ({{ get_coingecko_metrics("gmx") }})

select 
    date_spine.date as date
    , 'gmx' as app
    , 'DeFi' as category

    --Standardized Metrics
    , coalesce(gmx_dex_swaps_v1.spot_dau, 0) + coalesce(gmx_dex_swaps_v2.spot_dau, 0) as spot_dau
    , coalesce(gmx_dex_swaps_v1.spot_volume, 0) + coalesce(gmx_dex_swaps_v2.spot_volume, 0) as spot_volume
    , coalesce(gmx_dex_swaps_v1.spot_fees, 0) + coalesce(gmx_dex_swaps_v2.spot_fees, 0) as spot_fees
    , coalesce(gmx_dex_swaps_v1.spot_revenue, 0) + coalesce(gmx_dex_swaps_v2.spot_revenue, 0) as spot_revenue
    , coalesce(tvl_metrics_grouped.tvl, 0) as tvl
    , coalesce(treasury.net_treasury_usd, 0) as treasury_value
    , coalesce(net_treasury.net_treasury_usd, 0) as net_treasury_value
    , coalesce(treasury_native.treasury_native, 0) as treasury_value_native

    -- Market Data
    , market_metrics.price as price
    , market_metrics.market_cap
    , market_metrics.fdmc
    , market_metrics.token_turnover_circulating
    , market_metrics.token_turnover_fdv
    , market_metrics.token_volume
from date_spine
left join tvl_metrics_grouped using(date)
left join gmx_dex_swaps_v1 using(date)
left join gmx_dex_swaps_v2 using(date)
left join treasury using(date)
left join treasury_native using(date)
left join net_treasury using(date)
left join market_metrics using(date)
where date < to_date(sysdate())