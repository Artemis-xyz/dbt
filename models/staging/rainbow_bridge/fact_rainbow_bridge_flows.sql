{{ config(
    materialized="table",
) }}
with
    dim_contracts as (
        select distinct address, chain, category
        from {{ ref("dim_contracts_gold") }} 
        where category is not null and chain is not null
    ),

    volume_and_fees_by_chain_and_symbol as (
        select
            date_trunc('hour', block_timestamp) as hour,
            source_chain,
            destination_chain,
            coalesce(c.category, 'Not Categorized') as category,
            amount_usd
        from {{ ref("fact_rainbow_bridge_transfers") }} t
        left join dim_contracts c on lower(t.token_address) = lower(c.address) and c.chain = 'ethereum' --only using l1token
    )

select
    date_trunc('day', hour) as date,
    'rainbow_bridge' as app,
    source_chain,
    destination_chain,
    category,
    sum(amount_usd) as amount_usd,
    null as fee_usd
from volume_and_fees_by_chain_and_symbol
group by 1, 2, 3, 4, 5
order by date asc, source_chain asc