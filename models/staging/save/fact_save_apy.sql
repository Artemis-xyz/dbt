{{ config(
    materialized="table"
) }}

with base as (
  select
    parse_json(source_json) as json,
    extraction_date
  from {{ source("PROD_LANDING", "raw_save_lending") }}
),

flattened as (
  select
    value:reserve_id::string as reserve_id,
    value:symbol::string as symbol,
    value:response as data,
    extraction_date
  from base,
  lateral flatten(input => json)
),

extracted as (
    select
        reserve_id,
        symbol,
        data:rates:supplyInterest::float as apy,
        data:reserve:liquidity:availableAmount::float as available_amount,
        data:reserve:liquidity:borrowedAmountWads::float / 1e18 as borrow_amount,
        data:reserve:liquidity:mintDecimals::int as mint_decimals,
        extraction_date
    from flattened
)

select
    reserve_id as id,
    apy * 0.01 as apy,
    available_amount / pow(10, mint_decimals) as tvl,
    extraction_date as extraction_timestamp,
    concat(symbol, ' Main Pool') as name,
    'Lending' as type,
    'save' as protocol,
    'solana' as chain,
    array_construct(symbol) as symbol
from extracted
