{{ config(materialized="table") }}

-- note source and destination chain may be null if not provided above
select
    date,
    'wormhole' as app,
    source_chain,
    destination_chain,
    category,
    sum(amount_usd) as amount_usd,
    null as fee_usd
from {{ ref("stg_wormhole_flows") }}
where source_chain is not null and destination_chain is not null
group by 1, 2, 3, 4, 5
