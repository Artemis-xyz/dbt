{{ config(materialized="view") }}
with
    max_extraction as (
        select max(extraction_date) as max_date
        from {{ source("PROD_LANDING", "raw_coingecko_nft_price_mc_vol") }}
    ),
    nft_data as (
        select
            regexp_substr(
                source_url,
                'https://pro-api.coingecko.com/api/v3/nfts/([^/]*)',
                1,
                1,
                'e',
                1
            ) as coingecko_nft_id,
            parse_json(source_json) as data
        from {{ source("PROD_LANDING", "raw_coingecko_nft_price_mc_vol") }}
        where extraction_date = (select max_date from max_extraction)
    ),
    floor_price_usd as (
        select
            coingecko_nft_id,
            date(to_timestamp(value[0]::number / 1000)) as date,
            avg(value[1]::float) as floor_price_usd
        from nft_data, lateral flatten(input => data:floor_price_usd)
        group by coingecko_nft_id, date
    ),
    floor_price_native as (
        select
            coingecko_nft_id,
            date(to_timestamp(value[0]::number / 1000)) as date,
            avg(value[1]::float) as floor_price_native
        from nft_data, lateral flatten(input => data:floor_price_native)
        group by coingecko_nft_id, date
    ),
    h24_volume_usd as (
        select
            coingecko_nft_id,
            date(to_timestamp(value[0]::number / 1000)) as date,
            avg(CASE WHEN value[1] = '' THEN 0 ELSE value[1]::FLOAT END) AS h24_volume_usd
        from nft_data, lateral flatten(input => data:h24_volume_usd)
        group by coingecko_nft_id, date
    ),
    h24_volume_native as (
        select
            coingecko_nft_id,
            date(to_timestamp(value[0]::number / 1000)) as date,
            avg(value[1]::float) as h24_volume_native
        from nft_data, lateral flatten(input => data:h24_volume_native)
        group by coingecko_nft_id, date
    ),
    market_cap_usd as (
        select
            coingecko_nft_id,
            date(to_timestamp(value[0]::number / 1000)) as date,
            avg(value[1]::float) as market_cap_usd
        from nft_data, lateral flatten(input => data:market_cap_usd)
        group by coingecko_nft_id, date
    ),
    market_cap_native as (
        select
            coingecko_nft_id,
            date(to_timestamp(value[0]::number / 1000)) as date,
            avg(value[1]::float) as market_cap_native
        from nft_data, lateral flatten(input => data:market_cap_native)
        group by coingecko_nft_id, date
    )

select
    floor_price_usd.date,
    floor_price_usd.coingecko_nft_id,
    floor_price_usd.floor_price_usd as nft_floor_price_usd,
    floor_price_native.floor_price_native as nft_floor_price_native,
    h24_volume_usd.h24_volume_usd as nft_h24_volume_usd,
    h24_volume_native.h24_volume_native as nft_h24_volume_native,
    market_cap_usd.market_cap_usd as nft_market_cap_usd,
    market_cap_native.market_cap_native as nft_market_cap_native
from floor_price_usd
join floor_price_native using (coingecko_nft_id, date)
join h24_volume_usd using (coingecko_nft_id, date)
join h24_volume_native using (coingecko_nft_id, date)
join market_cap_usd using (coingecko_nft_id, date)
join market_cap_native using (coingecko_nft_id, date)
where market_cap_usd.market_cap_usd is not null  -- Coingecko returns bad data (null values) for some NFT market caps
order by date desc
