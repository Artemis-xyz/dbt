{{ config(
    materialized="incremental",
    unique_key=[
        'id',
        'extraction_timestamp',
    ],
) }}

with base as (
  select
    parse_json(source_json) as json,
    extraction_date
  from {{ source("PROD_LANDING", "raw_save_lending") }}
  {% if is_incremental() %}
      where extraction_date > (
        select dateadd('day', -1, max(extraction_timestamp)) from {{ this }}
      )
  {% endif %}
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
        f.symbol,
        data:rates:supplyInterest::float as apy,
        data:reserve:liquidity:availableAmount::float as available_amount,
        data:reserve:liquidity:borrowedAmountWads::float / 1e18 as borrow_amount,
        data:reserve:liquidity:mintDecimals::int as mint_decimals,
        p.link,
        extraction_date
    from flattened f
    inner join {{ ref("save_stablecoin_lending_ids") }} p
    on reserve_id = p.id
)

select
    reserve_id as id,
    concat(symbol, ' Main Pool') as name,
    apy * 0.01 as apy,
    available_amount / pow(10, mint_decimals) as tvl,
    array_construct(symbol) as symbol,
    'save' as protocol,
    'Lending' as type,
    'solana' as chain,
    link,
    extraction_date as extraction_timestamp
from extracted
