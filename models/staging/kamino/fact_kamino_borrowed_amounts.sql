with
    max_extraction as (
        select max(extraction_date) as max_date
        from {{ source("PROD_LANDING", "raw_kamino_borrowed_amounts") }}
    )
select
    left(value:date, 10)::date as date,
    value:mint_address::string as mint_address,
    value:borrow_amount::number as borrow_amount,
    value:decimals::number as decimals
from {{ source("PROD_LANDING", "raw_kamino_borrowed_amounts") }}, lateral flatten(input => parse_json(source_json))