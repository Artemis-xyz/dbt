{{ config(snowflake_warehouse="CELESTIA") }}

with
    max_extraction as (
        select max(extraction_date) as max_date
        from {{ source("PROD_LANDING", "raw_celestia_fees_for_blobs") }}
    ),
    latest_data as (
        select parse_json(source_json) as data
        from {{ source("PROD_LANDING", "raw_celestia_fees_for_blobs") }}
        where extraction_date = (select max_date from max_extraction)
    ),
    flattened_data as (
        select
            date(to_timestamp(value:block_date::number / 1000)) as date,
            value:"fees_for_blobspace_tia"::float as fees_for_blobs_tia
        from latest_data, lateral flatten(input => data)
    )
select date, fees_for_blobs_tia
from flattened_data
where date < to_date(sysdate())
order by date desc
