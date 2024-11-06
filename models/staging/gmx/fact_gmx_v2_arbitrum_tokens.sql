{{
    config(
        materialized = 'table',
        snowflake_warehouse = 'GMX'
    )
}}

with
    max_extraction as (
        select max(extraction_date) as max_date
        from {{ source("PROD_LANDING", "raw_gmx_arbitrum_index_token_metadata") }}
    ),
    latest_data as (
        select parse_json(source_json) as data
        from {{ source("PROD_LANDING", "raw_gmx_arbitrum_index_token_metadata") }}
        where extraction_date = (select max_date from max_extraction)
    ),
    flattened_data as (
        select
            date(to_timestamp(value:block_date::number / 1000)) as date,
            value:"token_address"::string as token_address,
            value:"symbol"::string as symbol,
            value:"name"::string as name
        from latest_data, lateral flatten(input => data)
    )
select date, token_address, symbol, name
from flattened_data
where date < to_date(sysdate())
order by date desc

