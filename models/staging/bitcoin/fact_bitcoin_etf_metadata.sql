{{
    config(
        materialized='table',
        snowflake_warehouse='BITCOIN'
    )
}}

-- Credit to @hildobby for the original version of this dataset: https://dune.com/data/dune.hildobby.dataset_bitcoin_etf_metadata

with
    max_extraction as (
        select max(extraction_date) as max_date
        from {{ source("PROD_LANDING", "raw_bitcoin_etf_metadata") }}
    )
select
    value:custodian::string as custodian,
    left(value:fee, 4)::float as fee,
    value:issuer::string as issuer,
    value:ticker::string as ticker,
    value:website::string as website
from
    {{ source("PROD_LANDING", "raw_bitcoin_etf_metadata") }},
    lateral flatten(input => parse_json(source_json))
where extraction_date = (select max_date from max_extraction)
