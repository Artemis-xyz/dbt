{{ config(materialized="table", snowflake_warehouse="BRIDGE_MD") }}

with  
    dim_contracts as (
        select distinct address, chain, category
        from {{ ref("dim_contracts_gold") }} 
        where category is not null and chain is not null
    )
    , transfers_data as (
        select
            dst_chain
            , src_block_timestamp
            , src_chain
            , src_address
            , src_token_address
            , t2.category as category
            , amount_sent
            , fees
            , coalesce(t.src_symbol, t.dst_symbol) as symbol
        from {{ref("fact_stargate_v2_transfers")}} t
        left join dim_contracts t2 on lower(t.src_token_address) = lower(t2.address) and src_chain = t2.chain
    )
select
    src_block_timestamp::date as date
    , 'stargate' as app
    , src_chain as source_chain
    , dst_chain as destination_chain
    , category
    , symbol
    , sum(amount_sent) as amount_usd
    , sum(fees) as fee_usd
from transfers_data
where src_block_timestamp::date < to_date(sysdate())
group by 1, 2, 3, 4, 5, 6


