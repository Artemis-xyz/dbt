{{
    config(
        materialized='table',
        snowflake_warehouse='ETHEREUM'
    )
}}

-- Credit to @hildobby for the original version of this model: https://dune.com/hildobby/eth-etfs

with
    max_extraction as (
        select max(extraction_date) as max_date
        from {{ source("PROD_LANDING", "raw_ethereum_etf_addresses") }}
    )
select
    value:address::string as address,
    value:inverse_values::string as inverse_values,
    value:issuer::string as issuer,
    value:track_inflow::string as track_inflow,
    value:track_outflow::string as track_outflow
from
    {{ source("PROD_LANDING", "raw_ethereum_etf_addresses") }},
    lateral flatten(input => parse_json(source_json))
where extraction_date = (select max_date from max_extraction)