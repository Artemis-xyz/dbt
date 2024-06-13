{{ config(snowflake_warehouse="CELESTIA") }}

with
    max_extraction as (
        select max(extraction_date) as max_date
        from {{ source("PROD_LANDING", "raw_celestia_namespaces_and_blob_size") }}
    ),
    latest_data as (
        select parse_json(source_json) as data
        from {{ source("PROD_LANDING", "raw_celestia_namespaces_and_blob_size") }}
        where extraction_date = (select max_date from max_extraction)
    ),
    flattened_data as (
        select
            date(to_timestamp(value:date::number / 1000)) as date,
            value:"unique_namespaces_count"::integer as unique_namespaces_count,
            value:"total_blob_size_mb"::float as total_blob_size_mb
        from latest_data, lateral flatten(input => data)
    )
select date, unique_namespaces_count, total_blob_size_mb
from flattened_data
where date < to_date(sysdate())
order by date desc
