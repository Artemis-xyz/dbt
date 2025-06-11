with
    bisq as (
        with max_extraction as (
            select max(extraction_date) as max_date
            from {{ source('PROD_LANDING', 'raw_bisq_daily_volume') }}
        )
        select
            value:date::date as date,
            value:volume_btc::float as bisq_volume_btc
        from
            {{ source('PROD_LANDING', 'raw_bisq_daily_volume') }},
            lateral flatten(input => parse_json(source_json))
        where extraction_date = (select max_date from max_extraction)
    ),
    lnexchange as (
        with max_extraction as (
            select max(extraction_date) as max_date
            from {{ source('PROD_LANDING', 'raw_lnexchange_daily_volume') }}
        )
        select
            value:date::date as date,
            value:volume_usd::number as lnexchange_volume
        from
            {{ source('PROD_LANDING', 'raw_lnexchange_daily_volume') }},
            lateral flatten(input => parse_json(source_json))
        where extraction_date = (select max_date from max_extraction)
    )
select
    coalesce(bisq.date, lnexchange.date) as date,
    sum((coalesce(bisq.bisq_volume_btc, 0) * eph.price) + coalesce(lnexchange_volume, 0)) as volume_usd
from bisq
full join lnexchange on bisq.date = lnexchange.date
left join {{source('BITCOIN_FLIPSIDE_PRICE', 'ez_prices_hourly')}} eph 
    on bisq.date = eph.hour and lower(eph.symbol) = 'btc'
where eph.name = 'bitcoin'
group by 1
order by date desc
