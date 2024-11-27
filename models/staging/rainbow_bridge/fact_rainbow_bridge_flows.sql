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
            block_timestamp::date as date,
            source_chain,
            destination_chain,
            coalesce(c.category, 'Not Categorized') as category,
            coalesce(amount_usd, 0) as usd_amount
        from {{ ref("fact_rainbow_bridge_transfers") }} t
        left join dim_contracts c on lower(t.token_address) = lower(c.address) and c.chain = 'ethereum' --only using l1token
    )

select
    date,
    'rainbow_bridge' as app,
    source_chain,
    destination_chain,
    category,
    sum(usd_amount) as amount_usd,
    null as fee_usd
from volume_and_fees_by_chain_and_symbol
group by 1, 2, 3, 4, 5
having amount_usd is not null