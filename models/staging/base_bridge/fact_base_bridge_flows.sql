with
    volume_and_fees_by_chain_and_symbol as (
        select
            date_trunc('hour', block_timestamp) as hour,
            source_chain,
            destination_chain,
            p.symbol,
            t.token_address,
            sum(
                (coalesce(amount, 0) / power(10, coalesce(p.decimals, 18)))
                * coalesce(price, 0)
            ) as amount_usd,
            sum(null) as fee_usd
        from {{ ref("fact_base_bridge_transfers") }} t
        left join
            ethereum_flipside.price.ez_hourly_token_prices p
            on date_trunc('hour', t.block_timestamp) = p.hour
            and t.token_address = p.token_address
        group by 1, 2, 3, 4, 5
    )

select
    date_trunc('day', hour) as date,
    'base_bridge' as app,
    source_chain,
    destination_chain,
    category,
    sum(amount_usd) as amount_usd,
    sum(fee_usd) as fee_usd
from volume_and_fees_by_chain_and_symbol
left join {{ ref("dim_contracts_gold") }} t2 on lower(token_address) = lower(t2.address)
group by 1, 2, 3, 4, 5
order by date asc, source_chain asc
