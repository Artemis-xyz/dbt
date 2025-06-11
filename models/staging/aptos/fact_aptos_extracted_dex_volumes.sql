with
    thalaswap as (
        with max_extraction as (
            select max(extraction_date) as max_date
            from {{ source('PROD_LANDING', 'raw_aptos_thalaswap_v2_daily_volume') }}
        )
        select
            value:date::date as date,
            value:volume_usd::number as thalaswap_volume
        from
            {{ source('PROD_LANDING', 'raw_aptos_thalaswap_v2_daily_volume') }},
            lateral flatten(input => parse_json(source_json))
        where extraction_date = (select max_date from max_extraction)
    ),
    cellana as (
        with max_extraction as (
            select max(extraction_date) as max_date
            from {{ source('PROD_LANDING', 'raw_aptos_cellana_daily_volume') }}
        )
        select
            value:date::date as date,
            value:volume_usd::number as cellana_volume
        from
            {{ source('PROD_LANDING', 'raw_aptos_cellana_daily_volume') }},
            lateral flatten(input => parse_json(source_json))
        where extraction_date = (select max_date from max_extraction)
    ),
    hyperion as (
        with max_extraction as (
            select max(extraction_date) as max_date
            from {{ source('PROD_LANDING', 'raw_aptos_hyperion_daily_volume') }}
        )
        select
            value:date::date as date,
            value:dailyVolume::number as hyperion_volume
        from
            {{ source('PROD_LANDING', 'raw_aptos_hyperion_daily_volume') }},
            lateral flatten(input => parse_json(source_json))
        where extraction_date = (select max_date from max_extraction)
    ),
    all_dates as (
        select date from thalaswap
        union
        select date from cellana
        union
        select date from hyperion
    )
select
    d.date,
    t.thalaswap_volume,
    c.cellana_volume,
    h.hyperion_volume
from all_dates d
left join thalaswap t on d.date = t.date
left join cellana c on d.date = c.date
left join hyperion h on d.date = h.date
order by d.date desc
