{{ config(snowflake_warehouse="WORMHOLE", materialized="table") }}

WITH prices as (
    {{ get_coingecko_prices_on_chains(['solana', 'berachain', 'ethereum', 'aptos', 'sui', 'bsc', 'polygon', 'arbitrum', 'base', 'avalanche', 'celo', 'terra', 'terra-2']) }}
),
chain_ids as ( 
    select id, chain from {{ref('fact_wormhole_chain_ids') }}
)

select
    ops.id,
    src_timestamp,
    src_tx_hash,
    src_from_address,
    src_to_address,
    dst_timestamp,
    dst_tx_hash,
    dst_from_address,
    dst_to_address,
    amount as amount_native,
    CASE 
        WHEN amount_adjusted > 0 THEN amount_adjusted
        else amount / POW(10, 
        case 
            when lower(token_address) in (
                lower('0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48'),
                lower('0xb8e2e2101ed11e9138803cd3e06e16dd19910647'),
                lower('0xaaaebe6fe48e54f431b0c390cfaf0b017d09d42d'),
                lower('0x476c5e26a75bd202a9683ffd34359c0cc15be0ff'),
                lower('0xdac17f958d2ee523a2206206994597c13d831ec7')
            ) then 8 else prices.decimals end
        )
    end as amount_adjusted,
    coalesce(ops.amount_adjusted * price, (amount * coalesce(price, 0)) / pow(10, 
    case 
        when lower(token_address) in (
            lower('0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48'),
            lower('0xb8e2e2101ed11e9138803cd3e06e16dd19910647'),
            lower('0xaaaebe6fe48e54f431b0c390cfaf0b017d09d42d'),
            lower('0x476c5e26a75bd202a9683ffd34359c0cc15be0ff'),
            lower('0xdac17f958d2ee523a2206206994597c13d831ec7')
        ) then 8 else prices.decimals end
    ), ops.amount_usd) as amount,
    coalesce(prices.symbol, lower(ops.symbol)) as symbol,
    case 
        when amount_adjusted > 0 and amount is not null then ROUND(LOG(10, amount/amount_adjusted),0)
        when lower(token_address) in (
            lower('0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48'),
            lower('0xb8e2e2101ed11e9138803cd3e06e16dd19910647'),
            lower('0xaaaebe6fe48e54f431b0c390cfaf0b017d09d42d'),
            lower('0x476c5e26a75bd202a9683ffd34359c0cc15be0ff'),
            lower('0xdac17f958d2ee523a2206206994597c13d831ec7')
        ) then 8 
        else prices.decimals end as decimals,
    app_ids,
    fee,
    fee_address,
    fee_chain,
    from_address,
    f_chain.chain as source_chain,
    to_address,
    t_chain.chain as destination_chain,
    token_address,
    token_chain.chain as token_chain,
    normalized_decimals,
    src_status,
    dst_status,
    payload,
    extraction_date,
    case when contains(coalesce(lower(prices.symbol), lower(ops.symbol)), 'usd') then 'Stablecoin' else 'Token' end as category
from {{ref('fact_wormhole_operations')}} as ops
left join prices on lower(ops.token_address) = lower(contract_address) and date = date_trunc('day', ops.src_timestamp)
left join chain_ids as f_chain on f_chain.id = ops.from_chain
left join chain_ids as t_chain on t_chain.id = ops.to_chain
left join chain_ids as token_chain on token_chain.id = ops.token_chain
where token_address is not null and lower(ops.token_address) <> lower('0xcc8fa225d80b9c7d42f96e9570156c65d6caaa25')
and (amount is not null or amount_adjusted is not null)