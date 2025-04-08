with thala_volume as (
    with thala_v1 as (
        select
            date(block_timestamp) as date, 
            greatest(sum(amount_in_usd), sum(amount_out_usd)) as volume_usd
        from {{ source('APTOS_FLIPSIDE', 'ez_dex_swaps') }}
        where platform ILIKE 'thala'
        group by date
        order by date desc 
    ) 
    select 
        coalesce(v2.date, v1.date) as date, 
        coalesce(v2.thalaswap_volume, 0) + coalesce(v1.volume_usd, 0) as volume_usd
    from {{ ref('fact_aptos_extracted_dex_volumes') }} as v2
    full join thala_v1 as v1
        on v2.date = v1.date 
    order by date desc
), 

hyperion_volume as (
    select
      date,
      coalesce(lag(hyperion_volume, 1) over (order by date desc), 0) as volume_usd
    from {{ ref('fact_aptos_extracted_dex_volumes') }}
    order by date desc
), 

liquidswap_volume as (
    with daily_volume as (
        select
            date(block_timestamp) as date,
            greatest(sum(amount_in_usd), sum(amount_out_usd)) as volume_usd
        from {{ source('APTOS_FLIPSIDE', 'ez_dex_swaps') }}
        where platform ILIKE 'liquidswap'
        group by date
        order by date desc 
    )
    select 
        date, 
        coalesce(lag(volume_usd, 1) over (order by date desc), 0) as volume_usd
    from daily_volume
    order by date desc
), 

sushi_volume as (
    with daily_volume as (
        select
            date(block_timestamp) as date,
            least(sum(amount_in_usd), sum(amount_out_usd)) as volume_usd
        from {{ source('APTOS_FLIPSIDE', 'ez_dex_swaps') }}
        where platform ILIKE 'sushi'
        group by date
        order by date desc 
    )
    select 
        date, 
        coalesce(lag(volume_usd, 1) over (order by date desc), 0) as volume_usd
    from daily_volume
    order by date desc
), 

pancake_volume as (
    with daily_volume as (
        select
            date(block_timestamp) as date,
            least(sum(amount_in_usd), sum(amount_out_usd)) as volume_usd
        from {{ source('APTOS_FLIPSIDE', 'ez_dex_swaps') }}
        where platform ILIKE 'pancake'
        group by date
        order by date desc 
    )
    select 
        date, 
        coalesce(lag(volume_usd, 1) over (order by date desc), 0) as volume_usd
    from daily_volume
    order by date desc
), 

cellana_volume as (
    select date, cellana_volume as volume_usd
    from {{ ref('fact_aptos_extracted_dex_volumes') }}
    order by date desc
), 

-- Build a list of all dates across our CTEs
all_dates as (
    select date from thala_volume
    union
    select date from hyperion_volume
    union
    select date from liquidswap_volume
    union
    select date from sushi_volume
    union
    select date from pancake_volume
    union
    select date from cellana_volume
)

select 
    d.date,
    coalesce(tv.volume_usd, 0) +
    coalesce(hv.volume_usd, 0) +
    coalesce(lv.volume_usd, 0) +
    coalesce(sv.volume_usd, 0) +
    coalesce(pv.volume_usd, 0) +
    coalesce(cv.volume_usd, 0) as volume_usd
from all_dates d
left join thala_volume tv on d.date = tv.date
left join hyperion_volume hv on d.date = hv.date
left join liquidswap_volume lv on d.date = lv.date
left join sushi_volume sv on d.date = sv.date
left join pancake_volume pv on d.date = pv.date
left join cellana_volume cv on d.date = cv.date
order by d.date desc