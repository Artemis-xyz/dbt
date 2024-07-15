{{ config(materialized="view") }}

with
    max_extraction as (
        select max(extraction_date) as max_date
        from {{ source("PROD_LANDING", "raw_parallel_finance_txns") }}
    ),
    raw as (
        select date(value:date) as date, value:"value"::integer as txns
        from
            {{ source("PROD_LANDING", "raw_parallel_finance_txns") }},
            lateral flatten(input => parse_json(source_json))
        where extraction_date = (select max_date from max_extraction)
    )
select raw.date, 'parallel_finance' as chain, txns
from raw
where raw.date < to_date(sysdate())
