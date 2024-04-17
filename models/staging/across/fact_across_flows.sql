{{
    config(
        materialized="table",
        snowflake_warehouse="BRIDGE_MD",
    )
}}
with

    dim_contracts as (
        select distinct address, chain, category
        from {{ ref("dim_contracts_gold") }} 
        where category is not null and chain is not null
    ),

    across_transfers_chain_mapping as (
        select 
            version,
            contract_address,
            block_timestamp,
            tx_hash,
            event_index,
            amount,
            depositor,
            recipient,
            destination_chain_id,
            destination_token,
            origin_chain_id,
            destination_token_symbol,
            t2.chain as destination_chain, 
            t3.chain as source_chain, 
            t4.category as destination_category
        from {{ ref("fact_across_transfers") }} t1
        left join {{ ref("dim_chain_ids")}} t2 on t1.destination_chain_id = t2.id
        left join {{ ref("dim_chain_ids")}} t3 on t1.origin_chain_id = t3.id
        left join dim_contracts t4 on lower(destination_token) = lower(address) and destination_chain = t2.chain
    ),

    distinct_tokens as (
        select distinct destination_token as token_address, destination_chain as chain
        from across_transfers_chain_mapping
        where destination_token is not null
    ),

    prices as (
        select *
        from ethereum_flipside.price.ez_hourly_token_prices
        where token_address in (select token_address from distinct_tokens where chain = 'ethereum')
        union
        select *
        from optimism_flipside.price.ez_hourly_token_prices
        where token_address in (select token_address from distinct_tokens where chain = 'optimism')
        union
        select *
        from arbitrum_flipside.price.ez_hourly_token_prices
        where token_address in (select token_address from distinct_tokens where chain = 'arbitrum')
        union
        select *
        from polygon_flipside.price.ez_hourly_token_prices
        where token_address in (select token_address from distinct_tokens where chain = 'polygon')
        union
        select *
        from base_flipside.price.ez_hourly_token_prices
        where token_address in (select token_address from distinct_tokens where chain = 'base')
    ),
    dim_zkSync_tokens as (
        select symbol, address, chain, category
        from
            (
                values
                    (
                        'DAI',
                        lower('0x6B175474E89094C44Da98b954EedeAC495271d0F'),
                        'ethereum',
                        'Stablecoin'
                    ),
                    (
                        'USDC',
                        lower('0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48'),
                        'ethereum',
                        'Stablecoin'
                    ),
                    (
                        'USDC.e',
                        lower('0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48'),
                        'ethereum',
                        'Stablecoin'
                    ),
                    (
                        'USDT',
                        lower('0xdAC17F958D2ee523a2206206994597C13D831ec7'),
                        'ethereum',
                        'Stablecoin'
                    ),
                    (
                        'WBTC',
                        lower('0x2260FAC5E5542a773Aa44fBCfeDf7C193bc2C599'),
                        'ethereum',
                        'Token'
                    ),
                    (
                        'WETH',
                        lower('0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2'),
                        'ethereum',
                        'Token'
                    )
            ) as t(symbol, address, chain, category)

    ),

    
    zksync_transfers as (
        select 
            version,
            contract_address,
            block_timestamp,
            tx_hash,
            event_index,
            amount,
            depositor,
            recipient,
            destination_chain_id,
            dim_zkSync_tokens.address as destination_token,
            origin_chain_id,
            destination_token_symbol,
            destination_chain, 
            source_chain, 
            dim_zkSync_tokens.category as destination_category
        from across_transfers_chain_mapping
        left join dim_zkSync_tokens on lower(destination_token_symbol) = lower(symbol)
        where destination_token_symbol is not null
    ),

    zksync_prices as (
        select hour, token_address, decimals, avg(price) as price
        from ethereum_flipside.price.ez_hourly_token_prices
        inner join dim_zkSync_tokens on lower(token_address) = lower(address)
        group by 1, 2, 3
    ),


    zksync_volume_by_chain_and_symbol as (
        select
            date_trunc('hour', block_timestamp) as hour,
            source_chain,
            destination_chain,
            destination_category,
            sum(
                coalesce((amount / power(10, p.decimals)) * price, 0)
            ) as amount_usd
        from zksync_transfers t
        left join
            zksync_prices p
            on date_trunc('hour', t.block_timestamp) = p.hour
            and t.destination_token = p.token_address
        group by 1, 2, 3, 4

    ),

    non_zksync_volume_by_chain_and_symbol as (
       select
            date_trunc('hour', block_timestamp) as hour,
            source_chain,
            destination_chain,
            destination_category,
            sum(
                coalesce((amount / power(10, p.decimals)) * price, 0)
            ) as amount_usd
        from across_transfers_chain_mapping t
        left join
            prices p
            on date_trunc('hour', t.block_timestamp) = p.hour
            and t.destination_token = p.token_address
        where
            t.destination_token_symbol is null
        group by 1, 2, 3, 4

    ),
    flows_by_token as (
        select
            date_trunc('day', hour) as date,
            source_chain,
            destination_chain,
            coalesce(destination_category, 'Not Categorized') as category,
            sum(amount_usd) as amount_usd
        from
            (
                select *
                from zksync_volume_by_chain_and_symbol

                union

                select *
                from non_zksync_volume_by_chain_and_symbol

            ) t
        group by 1, 2, 3, 4
    )
select
    date,
    'across' as app,
    source_chain,
    destination_chain,
    category,
    amount_usd,
    null as fee_usd
from flows_by_token
order by date desc, source_chain asc

