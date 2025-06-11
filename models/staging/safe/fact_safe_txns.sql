{{
    config(
        materialized="table",
        snowflake_warehouse="SAFE",
    )
}}

with
    max_extraction as (
        select max(extraction_date) as max_date
        from {{ source("PROD_LANDING", "raw_safe_txns") }}
        
    ),
    latest_data as (
        select parse_json(source_json) as data
        from {{ source("PROD_LANDING", "raw_safe_txns") }}
        where extraction_date = (select max_date from max_extraction)
    )
select
    f.value:block_date::date as date
    , f.value:num_txs::number as txns
from latest_data, lateral flatten(input => data) f
