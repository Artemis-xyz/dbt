{{ config(materialized="view", snowflake_warehouse="PARALLEL_FINANCE") }}

with
    max_extraction as (
        select max(extraction_date) as max_date
        from {{ source("PROD_LANDING", "raw_parallel_finance_daa") }}
    ),
    raw as (
        select date(value:date) as date, value:"value"::integer as daa
        from
            {{ source("PROD_LANDING", "raw_parallel_finance_daa") }},
            lateral flatten(input => parse_json(source_json))
        where extraction_date = (select max_date from max_extraction)
    )
select raw.date, 'parallel_finance' as chain, daa
from raw
where raw.date < to_date(sysdate())
