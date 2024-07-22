{{ config(materialized="view") }}
with
    max_extraction as (
        select max(extraction_date) as max_date
        from {{ source("PROD_LANDING", "raw_fuse_txns") }}
    ),
    data as (
        select date(value:date) as date, value:"value"::integer as txns
        from
            {{ source("PROD_LANDING", "raw_fuse_txns") }},
            lateral flatten(input => parse_json(source_json))
        where extraction_date = (select max_date from max_extraction)
    )
select date, 'fuse' as chain, txns
from data
where date < to_date(sysdate())
