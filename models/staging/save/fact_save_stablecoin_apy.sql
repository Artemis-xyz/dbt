{{ config(
    materialized="table"
) }}

with latest as (
  select
    id,
    max(extraction_timestamp) as max_timestamp
  from {{ ref("fact_save_apy") }}
  group by id
)

select
    f.extraction_timestamp as timestamp,
    f.id,
    f.name,
    f.apy,
    f.tvl,
    f.symbol,
    f.protocol,
    f.type,
    f.chain,
    p.link
from {{ ref("fact_save_apy") }} f
join latest l
on f.id = l.id
    and f.extraction_timestamp = l.max_timestamp
join {{ ref("save_stablecoin_lending_ids") }} p
on f.id = p.id
