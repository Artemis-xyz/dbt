{{ config(materialized="view", snowflake_warehouse="FUSE") }}
with
    max_extraction as (
        select max(extraction_date) as max_date
        from {{ source("PROD_LANDING", "raw_fuse_daa") }}
    ),
    data as (
        select date(value:date) as date, value:"value"::integer as daa
        from
            {{ source("PROD_LANDING", "raw_fuse_daa") }},
            lateral flatten(input => parse_json(source_json))
        where extraction_date = (select max_date from max_extraction)
    )
select date, 'fuse' as chain, daa
from data
where date < to_date(sysdate())
