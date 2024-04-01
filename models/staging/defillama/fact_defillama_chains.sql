{{ config(materialized="table") }}
with
    -- Chains that come up in /chains but all of the other endpoints return no data
    black_listed_chains as (
        select 'Op_Bnb' as name
        union all
        select 'Orai' as name
    ),
    max_extraction as (
        select max(extraction_date) as max_date
        from {{ source("PROD_LANDING", "raw_defillama_chain_data") }}
    ),
    chain_data as (
        select extraction_date::date as date, parse_json(source_json) as data
        from {{ source("PROD_LANDING", "raw_defillama_chain_data") }}
        where extraction_date = (select max_date from max_extraction)
    ),
    flattened_data as (
        select
            date,
            value:"gecko_id"::string as gecko_id,
            value:"tvl"::float as tvl,
            value:"tokenSymbol"::string as token_symbol,
            value:"cmcId"::string as cmc_id,
            value:"name"::string as name,
            value:"chainId"::int as chain_id
        from chain_data, lateral flatten(input => data)
    )

select *
from flattened_data
where name not in (select name from black_listed_chains)
