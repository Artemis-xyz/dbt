with
    max_extraction as (
        select max(extraction_date) as max_date
        from {{ source("PROD_LANDING", "raw_ethereum_verified_contracts") }}
    ),
    data as (
        select
            source_json:date::date as date,
            source_json:val::number as verified_contracts,
            source_json as source,
            'ethereum' as chain
        from {{ source("PROD_LANDING", "raw_ethereum_verified_contracts") }}
        where extraction_date = (select max_date from max_extraction)
    )
select date, verified_contracts, source, chain
from data
