with
    max_extraction as (
        select max(extraction_date) as max_date
        from {{ source("PROD_LANDING", "raw_near_rhea_volumes") }}
    )
select
    value:date::date as date,
    value:volume::number as volume
from
    {{ source("PROD_LANDING", "raw_near_rhea_volumes") }}, 
        lateral flatten(input => parse_json(source_json))
where extraction_date = (select max_date from max_extraction)