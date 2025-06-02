{{ config(materialized="table") }}

with base as (
  select
    source_json,
    extraction_date
  from {{ source("PROD_LANDING", "raw_pendle_sy") }}
),

flattened as (
  select
    value as pool,
    base.extraction_date
    from base,
  lateral flatten(input => parse_json(base.source_json))
),
extracted as (
    select
        pool:id::string as id,
        pool:response:maxBoostedApy::float as apy,
        pool:response:liquidity:usd::float as tvl,
        pool:response:swapFeeApy::float as fees,
        p.chain,
        p.symbol,
        p.link,
        extraction_date
    from flattened
    inner join pc_dbt_db.prod.pendle_stablecoin_pool_ids p
    on pool:id::string = p.id
)

select
    id
    , concat(
        case when symbol = 'USDFALCON' then 'USDf' else symbol end,
        ' SY Pool'
    ) as name
    , apy
    , tvl
    , fees
    , array_construct(symbol) as symbol
    , 'pendle' as protocol
    , 'Pool' as type
    , case when chain = 1 then 'ethereum' end as chain
    , link
    , extraction_date as extraction_timestamp
from extracted