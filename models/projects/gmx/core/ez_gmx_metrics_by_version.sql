{{
    config(
        materialized="table",
        snowflake_warehouse="GMX",
        database="gmx",
        schema="core",
        alias="ez_metrics_by_version",
    )
}}

with
    gmx_dex_swaps_v1 as (
        select 
            date,
            version,
            count(distinct sender) as spot_dau,
            sum(amountOut_usd) as spot_volume,
            sum(amount_fees_usd) as spot_fees,
            sum(amount_fees_usd) as spot_revenue
        from {{ ref("fact_gmx_v1_dex_swaps") }}
        group by 1, 2
    ),
    gmx_dex_swaps_v2 as (
        select 
            date,
            version,
            count(distinct sender) as spot_dau,
            sum(amountOut_usd) as spot_volume,
            sum(amount_fees_usd) as spot_fees,
            sum(amount_fees_usd) as spot_revenue
        from {{ ref("fact_gmx_v2_dex_swaps") }}
        group by 1, 2
    ),
    tvl_metrics as (
        select
            date,
            version,
            sum(tvl_token_adjusted) as tvl
        from {{ ref("fact_gmx_all_versions_tvl") }}
        where version = 'v2'
        group by 1, 2

        union all   

        select
            date,
            version,
            sum(tvl_token_adjusted) as tvl
        from {{ ref("fact_gmx_all_versions_tvl") }}
        where version = 'v1'
        group by 1, 2
    ),
    tvl_metrics_grouped as (
        select
            date,
            version,
            sum(tvl) as tvl
        from tvl_metrics
        group by 1, 2
    ),
    date_spine as (
        select date, version
        from {{ ref("dim_date_spine") }}
        cross join (select distinct version from tvl_metrics )
        where date between '2020-03-01' and to_date(sysdate())
    )

select 
    date_spine.date as date
    , date_spine.version
    , 'gmx' as app
    , 'DeFi' as category

    --Standardized Metrics
    , coalesce(gmx_dex_swaps_v1.spot_dau, 0) + coalesce(gmx_dex_swaps_v2.spot_dau, 0) as spot_dau
    , coalesce(gmx_dex_swaps_v1.spot_volume, 0) + coalesce(gmx_dex_swaps_v2.spot_volume, 0) as spot_volume
    , coalesce(gmx_dex_swaps_v1.spot_fees, 0) + coalesce(gmx_dex_swaps_v2.spot_fees, 0) as spot_fees
    , coalesce(gmx_dex_swaps_v1.spot_revenue, 0) + coalesce(gmx_dex_swaps_v2.spot_revenue, 0) as spot_revenue
    , coalesce(tvl_metrics_grouped.tvl, 0) as tvl

from date_spine
left join tvl_metrics_grouped using(date, version)
left join gmx_dex_swaps_v1 using(date, version)
left join gmx_dex_swaps_v2 using(date, version)
where date < to_date(sysdate())