{{config(
    materialized = 'table',
    database = 'leo', 
    snowflake_warehouse = 'leo'
)}}

with
    max_extraction as (
        select max(extraction_date) as max_date
        from {{ source("PROD_LANDING", "raw_leo_burn_history") }}
    )

select
    value:date::date as date,
    value:timestamp_ms::number as timestamp_ms,
    value:chain::string as chain,
    value:tx_hash::string as tx_hash,
    value:leo_burn_amount::number as leo_burn_amount
from
    {{ source("PROD_LANDING", "raw_leo_burn_history") }},
    lateral flatten(input => parse_json(source_json))
where extraction_date = (select max_date from max_extraction)