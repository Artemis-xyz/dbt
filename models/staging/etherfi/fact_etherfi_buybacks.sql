{{
    config(
        materialized="table",
        snowflake_warehouse="ETHERFI",
    )
}}

{{
    config(
        materialized="table",
        snowflake_warehouse="VIRTUALS",
    )
}}

with
    max_extraction as (
        select max(extraction_date) as max_date
        from {{ source("PROD_LANDING", "raw_etherfi_buybacks") }}
        
    ),
    latest_data as (
        select parse_json(source_json) as data
        from {{ source("PROD_LANDING", "raw_etherfi_buybacks") }}
        where extraction_date = (select max_date from max_extraction)
    )
select
    to_date(to_timestamp_ntz(replace(f.value:hour::string, ' UTC', ''))) as date
    , f.value:cum_ethfi_bought::number as cumulative_ethfi_bought
    , f.value:ethfi_bought::number as ethfi_bought
    , 'etherfi' as app
    , 'ethereum' as chain
    , 'DeFi' as category
from latest_data, lateral flatten(input => data) f
