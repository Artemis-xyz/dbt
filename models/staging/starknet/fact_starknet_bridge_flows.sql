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
            p.symbol,
            coalesce((amount / power(10, p.decimals)) * price, 0) as amount_usd
        from {{ ref("fact_starknet_bridge_transfers") }} t
        left join
            ethereum_flipside.price.ez_prices_hourly p
            on date_trunc('hour', t.block_timestamp) = p.hour
            and t.token_address = p.token_address
        left join dim_contracts c on lower(t.token_address) = lower(c.address) and c.chain = 'ethereum' --only using l1token
    )

select
    date_trunc('day', hour) as date,
    'starknet' as app,
    source_chain,
    destination_chain,
    category,
    symbol,
    sum(coalesce(amount_usd, 0)) as amount_usd,
    null as fee_usd
from volume_and_fees_by_chain_and_symbol
group by 1, 2, 3, 4, 5, 6
order by date asc, source_chain asc
