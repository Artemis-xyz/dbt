{{
    config(
        materialized="table",
        snowflake_warehouse="ONDO",
    )
}}

with
    max_extraction as (
        select max(extraction_date) as max_date
        from {{ source("PROD_LANDING", "raw_ondo_ousg_fees") }}
        
    ),
    latest_data as (
        select parse_json(source_json) as data
        from {{ source("PROD_LANDING", "raw_ondo_ousg_fees") }}
        where extraction_date = (select max_date from max_extraction)
    )
select
    f.value:Date::date as date,
    f.value:"Daily Management Fee"::number as fee
from latest_data, lateral flatten(input => data) f