{{ config(materialized="table") }}
select
    to_timestamp(block_timestamp)::date as date,
    coalesce(count(*), 0) as txns,
    'zora' as chain
from {{ ref("fact_zora_transactions") }}
where
    lower(to_address) <> lower('0x4200000000000000000000000000000000000015')
    and receipt_status = 1
group by 1
order by 1 asc
