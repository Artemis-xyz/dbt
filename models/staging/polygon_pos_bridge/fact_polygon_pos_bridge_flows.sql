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
            p.symbol,
            coalesce(c.category, 'Not Categorized') as category,
            coalesce((amount / power(10, p.decimals)) * price, 0) as amount_usd
        from {{ ref("fact_polygon_pos_bridge_transfers") }} t
        left join
            ethereum_flipside.price.ez_prices_hourly p
            on date_trunc('hour', t.block_timestamp) = p.hour
            and t.token_address = p.token_address
        left join dim_contracts c on lower(t.token_address) = lower(c.address) and c.chain = 'ethereum' --only using l1token
    ),

    filtered_volume_and_fees_by_chain_and_symbol as (
        select
            hour,
            source_chain,
            destination_chain,
            symbol,
            category,
            case
                when (symbol in ('DG', 'BORING', 'TORG')) and amount_usd > 10000000
                then 0
                else amount_usd
            end as amount_usd
        from volume_and_fees_by_chain_and_symbol
    )

select
    date_trunc('day', hour) as date,
    'polygon' as app,
    source_chain,
    destination_chain,
    category,
    sum(amount_usd) as amount_usd,
    null as fee_usd
from filtered_volume_and_fees_by_chain_and_symbol
group by 1, 2, 3, 4, 5
order by date asc, source_chain asc
