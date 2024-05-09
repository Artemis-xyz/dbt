with
    max_extraction as (
        select max(extraction_date) as max_date
        from {{ source("PROD_LANDING", "raw_ton_txns") }}
    ),
    latest_data as (
        select parse_json(source_json) as data
        from {{ source("PROD_LANDING", "raw_ton_txns") }}
        where extraction_date = (select max_date from max_extraction)
    ),
    flattened_data as (
        select value:"timestamp"::date as date, value:"value"::number as txns
        from latest_data, lateral flatten(input => data)
    )
select date, txns, 'ton' as chain
from flattened_data