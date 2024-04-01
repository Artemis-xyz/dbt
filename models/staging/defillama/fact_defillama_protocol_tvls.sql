{{ config(materialized="table") }}
with
    max_extraction as (
        select max(extraction_date) as max_date
        from {{ source("PROD_LANDING", "raw_defillama_protocol_tvls") }}
    ),
    latest_data as (
        select extraction_date::date as extraction_date, parse_json(source_json) as data
        from {{ source("PROD_LANDING", "raw_defillama_protocol_tvls") }}
        where extraction_date = (select max_date from max_extraction)
    ),
    flattened_data as (
        select
            extraction_date,
            data:id::string as defillama_protocol_id,
            to_date(convert_timezone('UTC', tvl.value:"date"::timestamp)) as date,
            tvl.value:"totalLiquidityUSD"::float as tvl
        from latest_data, lateral flatten(input => data:tvl) as tvl
    )

select defillama_protocol_id, date, tvl
from flattened_data
