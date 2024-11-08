{{
    config(
        materialized = 'table',
        snowflake_warehouse = 'GMX'
    )
}}

with
    max_extraction as (
        select max(extraction_date) as max_date
        from {{ source("PROD_LANDING", "raw_gmx_avalanche_index_token_metadata") }}
    ),
    latest_data as (
        select parse_json(source_json) as data
        from {{ source("PROD_LANDING", "raw_gmx_avalanche_index_token_metadata") }}
        where extraction_date = (select max_date from max_extraction)
    ),
    flattened_data as (
        select
            value:address::string as address,
            value:decimals::number as decimals,
            value:symbol::string as symbol
        from latest_data, lateral flatten(input => data:tokens)
    )
select address, decimals, symbol
from flattened_data
