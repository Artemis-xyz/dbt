{{config(materialized="view")}}
with 
    max_extraction as (
        select max(extraction_date) as max_date
        from {{ source("PROD_LANDING", "raw_coingecko_token_metadata") }}
    )
select
    parse_json(source_json) as json,
    json:id::string AS coingecko_token_id,
    json:symbol::string AS symbol,
    --TODO: Right now chains are coingecko chains, we need to map them to our chains
    f.key::string AS chain,
    f.value:contract_address::string AS contract_address,
    f.value:decimal_place::number AS decimals
from {{ source("PROD_LANDING", "raw_coingecko_token_metadata") }},
    lateral flatten(input => parse_json(source_json):detail_platforms) f
where extraction_date = (select max_date from max_extraction) and (
    chain <> '' and chain is not null
    and contract_address <> '' and chain is not null
    and decimals is not null
)