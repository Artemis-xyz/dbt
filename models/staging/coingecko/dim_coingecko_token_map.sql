{{config(materialized="view")}}
with 
    max_extraction as (
        select max(extraction_date) as max_date
        from {{ source("PROD_LANDING", "raw_coingecko_token_metadata") }}
    )
    , token_metadata as (
        select
            parse_json(source_json) as json,
            json:id::string AS coingecko_token_id,
            json:symbol::string AS symbol,
            --TODO: Right now chains are coingecko chains, we need to map them to our chains
            case 
                when f.key::string = 'binance-smart-chain' then 'bsc'
                when f.key::string = 'arbitrum-one' then 'arbitrum'
                when f.key::string = 'optimistic-ethereum' then 'optimism'
                when f.key::string = 'polygon-pos' then 'polygon'
                when f.key::string = 'sei-v2' then 'sei'
                when f.key::string = 'world-chain' then 'worldchain'
                when f.key::string = 'plume-network' then 'plume'
                else f.key::string
            end AS chain,
            f.value:contract_address::string AS address,
            f.value:decimal_place::number AS decimals
        from {{ source("PROD_LANDING", "raw_coingecko_token_metadata") }},
            lateral flatten(input => parse_json(source_json):detail_platforms) f
        where extraction_date = (select max_date from max_extraction) and (
            chain <> '' and chain is not null
            and address <> '' and chain is not null
            and decimals is not null
        )
        union 
        select null as json, coingecko_token_id, symbol, chain, contract_address as address, decimals
        from {{ ref("manually_added_tokens_seed") }}
    )
select coingecko_token_id, chain, lower(address) as contract_address, max(json) as json, max(symbol) as symbol,  max(decimals) as decimals
from token_metadata
group by coingecko_token_id, chain, contract_address