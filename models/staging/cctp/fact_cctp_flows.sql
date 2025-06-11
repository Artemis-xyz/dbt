{{ config(
    materialized="table",
)}}
with
    volume_by_chain_and_symbol as (
        select
            block_timestamp::date as date,
            src_chain as source_chain,
            dst_chain as destination_chain,
            'Stablecoin' as category,
            amount_usd
        from {{ ref("fact_cctp_transfers")}} t
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