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
  from {{ source("PROD_LANDING", "raw_kamino_lending") }}
  {% if is_incremental() %}
    where extraction_date > (
      select dateadd('day', -1, max(extraction_timestamp)) from {{ this }}
    )
  {% endif %}
),

historical as (
  select
    outer_item.value:market_id::string as market_id,
    outer_item.value:reserve_id::string as reserve_id,
    history_item.value:timestamp::timestamp_ntz as timestamp,
    history_item.value:metrics:symbol::string as symbol,
    history_item.value:metrics:supplyInterestAPY::float as supply_interest_apy,
    history_item.value:metrics:depositTvl::float as deposit_tvl,
    history_item.value:metrics:borrowTvl::float as borrow_tvl,
    b.extraction_date as extraction_date
  from base b,
  lateral flatten(input => b.json) as outer_item,
  lateral flatten(input => outer_item.value:response:history) as history_item
  qualify row_number() over (
    partition by
      market_id,
      reserve_id,
      b.extraction_date
    order by timestamp desc
  ) = 1
),

extracted as (
  select
    h.market_id,
    h.reserve_id,
    h.symbol,
    h.supply_interest_apy,
    h.deposit_tvl,
    h.borrow_tvl,
    h.extraction_date,
    l.link
  from historical h
  inner join {{ ref("kamino_stablecoin_lending_ids") }} l
  on h.market_id = l.market_id
    and h.reserve_id = l.id

)

select
    market_id,
    reserve_id as id,
    case
      when market_id = '7u3HeHxYDLhnCoErrtycNokbQYbWGzLs6JSDqGAv5PfF' then concat(symbol, ' Main Market')
      when market_id = 'DxXdAyU3kCjnyggvHmY5nAwg5cRbbmdyX3npfDMjjMek' then concat(symbol, ' JLP Market')
      else symbol
    end as name,
    supply_interest_apy as apy,
    deposit_tvl - borrow_tvl as tvl,
    array_construct(symbol) as symbol,
    'kamino' as protocol,
    'Lending' as type,
    'solana' as chain,
    link,
    extraction_date as extraction_timestamp
from extracted