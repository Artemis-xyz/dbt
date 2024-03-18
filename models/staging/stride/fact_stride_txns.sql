with
    max_extraction as (
        select max(extraction_date) as max_date
        from {{ source("PROD_LANDING", "raw_stride_txns") }}
    ),
    latest_data as (
        select parse_json(source_json) as data
        from {{ source("PROD_LANDING", "raw_stride_txns") }}
        where extraction_date = (select max_date from max_extraction)
    ),
    flattened_data as (
        select to_date(value:date::string) as date, value:"txns"::float as txns
        from latest_data, lateral flatten(input => data)
    )
select date, txns, 'stride' as chain
from flattened_data
