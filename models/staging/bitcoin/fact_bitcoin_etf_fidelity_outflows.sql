{{
    config(
        materialized='table',
        snowflake_warehouse='BITCOIN'
    )
}}

-- Credit to @hildobby for the original version of this model: https://dune.com/hildobby/btc-etfs

with
    max_extraction as (
        select max(extraction_date) as max_date
        from {{ source("PROD_LANDING", "raw_bitcoin_fidelity_outflows") }}
    )
select
    value:date::date as date,
    value:amount::float as amount
from
    {{ source("PROD_LANDING", "raw_bitcoin_fidelity_outflows") }},
    lateral flatten(input => parse_json(source_json))
where extraction_date = (select max_date from max_extraction)
