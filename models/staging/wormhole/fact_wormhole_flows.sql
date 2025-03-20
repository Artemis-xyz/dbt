{{ config(materialized="table") }}

-- note source and destination chain may be null if not provided above
select
    date_trunc('day', src_timestamp) as date,
    'wormhole' as app,
    source_chain,
    destination_chain,
    category,
    sum(amount) as amount_usd,
    null as fee_usd
from {{ ref("fact_wormhole_operations_with_price") }}
where source_chain is not null and destination_chain is not null and amount is not null
group by 1, 2, 3, 4, 5
