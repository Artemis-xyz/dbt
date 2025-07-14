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
            f.value:decimal_place::number AS decimals,
            1 as priority
        from {{ source("PROD_LANDING", "raw_coingecko_token_metadata") }},
            lateral flatten(input => parse_json(source_json):detail_platforms) f
        where extraction_date = (select max_date from max_extraction) and (
            chain <> '' and chain is not null
            and address <> '' and chain is not null
            and decimals is not null
        )
        union 
        select
            null as json
            , coingecko_token_id
            , symbol
            , chain
            , contract_address as address
            , decimals
            , 2 as priority
        from {{ ref("manually_added_tokens_seed") }}
    )
    , stellar_adjusted as (
        select
            coingecko_token_id,
            chain,
            case
                when lower(chain) = 'stellar' then
                -- Coingecko contract address data have 3 variants for Stellar that we want to standardise to 'symbol-issuer':
                -- 1. 'symbol:issuer'
                    (case when lower(address) like '%:%' then
                        lower(REPLACE(address, ':', '-'))
                -- 2. 'symbol-issuer'
                    when lower(address) like '%-%' then
                        lower(address)
                    else
                -- 3. 'issuer' (no symbol)
                        lower(symbol || '-' || address)
                    end)
                else lower(address)
            end as address,
            json,
            symbol,
            case when chain = 'stellar' then 7 else decimals end as decimals,
            priority
            -- Stellar tokens are stored as ONLY 7 decimals on the ledger.
        from token_metadata
    )
select 
    coingecko_token_id
    , chain
    , lower(address) as contract_address
    , json
    , symbol
    , decimals
from stellar_adjusted
qualify row_number() over (partition by lower(chain), lower(address) order by priority) = 1