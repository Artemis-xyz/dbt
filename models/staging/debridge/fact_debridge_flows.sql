{{ config(snowflake_warehouse="DEBRIDGE", materialized="table") }}

-- note source and destination chain may be null if not provided above
select
    date_trunc('day', src_timestamp) as date,
    'debridge' as app,
    source_chain,
    destination_chain,
    category,
    sum(coalesce(amount_sent, amount_received, 0)) as amount_usd,
    sum(coalesce(percentage_fee,0) + coalesce(fix_fee,0)) as fee_usd
from {{ ref("fact_debridge_transfers_with_prices") }}
where source_chain is not null and destination_chain is not null
group by 1, 2, 3, 4, 5
