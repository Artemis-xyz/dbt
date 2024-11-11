with
    max_extraction as (
        select max(extraction_date) as max_date
        from {{ source("PROD_LANDING", "raw_stellar_stablecoin_tvl") }}
    ),
    latest_data as (
        select parse_json(source_json) as data
        from {{ source("PROD_LANDING", "raw_stellar_stablecoin_tvl") }}
        where extraction_date = (select max_date from max_extraction)
    ),
    flattened_data as (
        select
            DATE(to_timestamp(f.value:date)) as date,
            f.value:gecko_id::text as gecko_id,
            f.value:symbol::text as symbol,
            f.value:peg_type::text as peg_type,
            f.value:totalCirculating::float as total_circulating,
            f.value:totalCirculatingUSD::float as total_circulating_usd
        from latest_data, lateral flatten(input => data) as f
    )
select date, gecko_id, symbol, peg_type, total_circulating, total_circulating_usd, 'stellar' as chain
from flattened_data
where date < to_date(sysdate())
order by date desc
