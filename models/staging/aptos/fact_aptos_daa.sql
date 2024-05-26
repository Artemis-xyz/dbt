with
    max_extraction_date as (
        select max(extraction_date) as max_date
        from {{ source("PROD_LANDING", "raw_aptos_daa") }}
    )
select
    value:date::date as date,
    value:"num_user_transactions"::number as daa
from {{ source("PROD_LANDING", "raw_aptos_daa") }},
lateral flatten(input => source_json)
where extraction_date = (select max_date from max_extraction_date)
