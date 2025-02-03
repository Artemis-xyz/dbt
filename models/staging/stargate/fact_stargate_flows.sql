{{ config(materialized="table") }}

with  
    dim_contracts as (
        select distinct address, chain, category
        from {{ ref("dim_contracts_gold") }} 
        where category is not null and chain is not null
    )
    , transfers_data as (
        select
            version
            , src_timestamp
            , src_hash
            , src.chain as source_chain
            , depositor
            , src_token_address
            , category
            , dst_timestamp
            , dst.chain as destination_chain
            , recipient
            , usd_value
            , coalesce(treasury_fee, 0) as treasury_fee
        from {{ref("fact_stargate_transfers")}} t
        left join {{ ref("stargate_chain_ids") }} src on src_chain = src.id
        left join {{ ref("stargate_chain_ids") }} dst on dst_chain = dst.id
        left join dim_contracts t2 on lower(t.src_token_address) = lower(t2.address) and src.chain = t2.chain
    )

select
    src_timestamp as block_timestamp
    , 'stargate' as app
    , source_chain
    , destination_chain
    , category
    , sum(usd_value) as amount_usd
    , sum(treasury_fee) as treasury_fee_usd
from transfers_data
where src_timestamp::date < to_date(sysdate())
group by 1, 2, 3, 4, 5


