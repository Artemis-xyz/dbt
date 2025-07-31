{{ config(materialized="table", snowflake_warehouse="BRIDGE_MD") }}


select
    src_block_timestamp::date as date
    , 'usdt0' as app
    , src_chain as source_chain
    , dst_chain as destination_chain
    , 'Stablecoin' as category
    , sum(amount_sent) as amount_usd
    , 0 as fee_usd
from {{ref("fact_usdt0_transfers")}}
where src_block_timestamp::date < to_date(sysdate())
group by 1, 2, 3, 4, 5