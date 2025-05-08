{{ config(
    materialized="table"
) }}

with base as (
  select
    parse_json(source_json) as json,
    extraction_date
  from {{ source("PROD_LANDING", "raw_kamino_lending") }}
),

outer_flatten as (
  select
    value:reserve_id::string as reserve_id,
    value:market_id::string as market_id,
    value:response:history as history_array,
    extraction_date
  from base,
  lateral flatten(input => json)
),

history_flattened as (
  select
    market_id,
    reserve_id,
    value:timestamp::timestamp_ntz as timestamp,
    value:metrics:symbol::string as symbol,
    value:metrics:supplyInterestAPY::float as supply_interest_apy,
    value:metrics:depositTvl::float as deposit_tvl,
    value:metrics:borrowTvl::float as borrow_tvl,
    extraction_date
  from outer_flatten,
  lateral flatten(input => history_array)
),

latest_timestamp_per_reserve as (
  select
    market_id,
    reserve_id,
    max(timestamp) as max_timestamp
  from history_flattened
  group by market_id, reserve_id
)

select
    h.market_id,
    h.reserve_id as id,
    h.supply_interest_apy as apy,
    h.deposit_tvl - h.borrow_tvl as tvl,
    h.extraction_date as extraction_timestamp,
    case
    when h.market_id = '7u3HeHxYDLhnCoErrtycNokbQYbWGzLs6JSDqGAv5PfF' then concat(h.symbol, ' Main Market')
    when h.market_id = 'DxXdAyU3kCjnyggvHmY5nAwg5cRbbmdyX3npfDMjjMek' then concat(h.symbol, ' JLP Market')
    else h.symbol
    end as name,
    'Lending' as type,
    'kamino' as protocol,
    'solana' as chain,
    array_construct(h.symbol) as symbol
from history_flattened h
join latest_timestamp_per_reserve l
    on h.market_id = l.market_id
    and h.reserve_id = l.reserve_id
    and h.timestamp = l.max_timestamp