{{ config(materialized="table") }}
select
    to_timestamp(block_timestamp)::date as date,
    coalesce(count(distinct from_address), 0) as daa,
    'linea' as chain
from {{ ref("fact_linea_transactions") }}
where
    lower(to_address) <> lower('0x508Ca82Df566dCD1B0DE8296e70a96332cD644ec')
    and receipt_status = 1
group by 1
order by 1 asc
