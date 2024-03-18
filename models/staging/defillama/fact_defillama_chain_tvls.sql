{{ config(materialized="table") }}
with
    max_extraction as (
        select max(extraction_date) as max_date
        from {{ source("PROD_LANDING", "raw_defillama_chain_tvls") }}
    ),
    latest_data as (
        select
            extraction_date::date as extraction_date,
            source_url,
            parse_json(source_json) as data
        from {{ source("PROD_LANDING", "raw_defillama_chain_tvls") }}
        where extraction_date = (select max_date from max_extraction)
    ),
    flattened_data as (
        select
            extraction_date,
            case
                when
                    lower(
                        split_part(source_url, '/', array_size(split(source_url, '/')))
                    )
                    = 'binance'
                then 'bsc'
                else
                    lower(
                        split_part(source_url, '/', array_size(split(source_url, '/')))
                    )
            end as defillama_chain_name,
            to_date(convert_timezone('UTC', value:"date"::timestamp)) as date,
            value:"tvl"::float as tvl
        from latest_data, lateral flatten(input => data)
    )

select date, tvl, defillama_chain_name
from flattened_data
