{{ config(materialized="view") }}
with
    max_extraction as (
        select max(extraction_date) as max_date
        from {{ source("PROD_LANDING", "raw_cardano_daa_partitioned") }}
    ),
    cardano_data as (
        select parse_json(source_json) as data
        from {{ source("PROD_LANDING", "raw_cardano_daa_partitioned") }}
        where extraction_date = (select max_date from max_extraction)
    )
select date(value[0]) as date, value[1] as daa, value as source, 'cardano' as chain
from cardano_data, lateral flatten(input => data:data: values)
