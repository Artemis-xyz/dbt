{{
    config(
        materialized="incremental",
        snowflake_warehouse= "STABLECOIN_DAILY",
    )
}}

select
    date_trunc('day', date) as date_granularity
    , case
        when subregion in (
            'northern_europe', 'southern_europe', 'eastern_europe', 'east_europe',
            'central_europe', 'southeast_europe', 'west_europe'
        ) then 'europe'

        when subregion in (
            'north_africa', 'east_africa', 'west_africa',
            'central_africa', 'southern_africa'
        ) then 'africa'

        when subregion = 'north_america' then 'north_america'

        when subregion in ('south_america', 'central_america', 'caribbean') then 'latin_america'

        when subregion = 'southeast_asia' then 'southeast_asia'

        when subregion = 'oceania' then 'oceania'

        when subregion in (
            'central_asia', 'south_asia', 'west_asia', 'east_asia', 'middle_east'
        ) then 'asia'
        end as subregion
    , t1.chain
    , count(distinct case when artemis_stablecoin_daily_txns > 0 then t1.address end) as artemis_stablecoin_dau
    , sum(artemis_stablecoin_daily_txns) as artemis_stablecoin_daily_txns
    , sum(artemis_stablecoin_transfer_volume) AS artemis_stablecoin_transfer_volume
from {{ ref("agg_daily_stablecoin_breakdown_with_labels_silver") }} t1
left join pc_dbt_db.prod.dim_geo_labels t2 
on lower(t1.address) = lower(t2.address) 
and t1.chain = t2.chain
where t1.chain in ('ethereum', 'solana') and date_granularity > '2020-01-01'
and subregion is not null
{% if is_incremental() %}
    and date_granularity >= (select DATEADD('day', -3, max(date_granularity)) from {{ this }})
{% endif %}
group by 1, 2, 3
