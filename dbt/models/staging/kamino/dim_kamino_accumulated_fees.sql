with
    max_extraction as (
        select max(extraction_date) as max_date
        from {{ source("PROD_LANDING", "raw_kamino_accumulated_fees") }}
    )
select
    left(value:date, 10)::date as date,
    value:mint_address::string as mint_address,
    value:accumulated_fees::number as accumulated_fees
from
    {{ source("PROD_LANDING", "raw_kamino_accumulated_fees") }},
    lateral flatten(input => parse_json(source_json))
where extraction_date = (select max_date from max_extraction)
