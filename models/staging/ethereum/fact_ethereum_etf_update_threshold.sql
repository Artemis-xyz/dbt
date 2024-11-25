{{
    config(
        materialized='table',
        snowflake_warehouse='ETHEREUM'
    )
}}

-- Credit to @hildobby for the original version of this dataset: https://dune.com/data/dune.hildobby.dataset_etf_update_thresholds

with
    max_extraction as (
        select max(extraction_date) as max_date
        from {{ source("PROD_LANDING", "raw_etf_update_thresholds") }}
    )
select
    left(value:ethereum, 10)::date as ethereum_update_threshold
from
    {{ source("PROD_LANDING", "raw_etf_update_thresholds") }},
    lateral flatten(input => parse_json(source_json))
where extraction_date = (select max_date from max_extraction)
