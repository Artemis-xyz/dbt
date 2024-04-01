with
    max_extraction as (
        select max(extraction_date) as max_date
        from {{ source("PROD_LANDING", "raw_starknet_txns") }}
    )
select value:"date"::date as date, value:"value"::int as txns, 'starknet' as chain
from
    {{ source("PROD_LANDING", "raw_starknet_txns") }},
    lateral flatten(input => parse_json(source_json))
where extraction_date = (select max_date from max_extraction)
