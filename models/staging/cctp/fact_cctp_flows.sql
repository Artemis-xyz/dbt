{{ config(
    materialized="table",
)}}
with
    chain_id_map as (
        select chain_id, chain
        from  (
            values
                (0, 'ethereum'),
                (1, 'avalanche'),
                (2, 'optimism'),
                (3, 'arbitrum'),
                (4, 'noble'),
                (5, 'solana'),
                (6, 'base'),
                (7, 'polygon')
                
        ) as t(chain_id, chain)
    ),
    volume_by_chain_and_symbol as (
        select
            block_timestamp::date as date,
            c1.chain as source_chain,
            c2.chain as destination_chain,
            'Stablecoin' as category,
            amount_usd
        from {{ ref("fact_cctp_transfers")}} t
        left join chain_id_map c1 on t.source_domain_id = c1.chain_id
        left join chain_id_map c2 on t.destination_domain_id = c2.chain_id
    )
select
    date,
    'cctp' as app,
    source_chain,
    destination_chain,
    category,
    sum(amount_usd) as amount_usd,
    null as fee_usd
from volume_by_chain_and_symbol
where source_chain is not null and destination_chain is not null
group by 1, 2, 3, 4, 5