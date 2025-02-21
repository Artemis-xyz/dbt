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
        from ethereum_flipside.price.ez_prices_hourly
        where token_address in (select token_address from distinct_tokens where chain = 'ethereum')
        union
        select *
        from optimism_flipside.price.ez_prices_hourly
        where token_address in (select token_address from distinct_tokens where chain = 'optimism')
        union
        select *
        from arbitrum_flipside.price.ez_prices_hourly
        where token_address in (select token_address from distinct_tokens where chain = 'arbitrum')
        union
        select *
        from polygon_flipside.price.ez_prices_hourly
        where token_address in (select token_address from distinct_tokens where chain = 'polygon')
        union
        select *
        from base_flipside.price.ez_prices_hourly
        where token_address in (select token_address from distinct_tokens where chain = 'base')
    ),
    dim_tokens as (
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
                    ),
                    (
                        'WETH',
                        lower('0x4200000000000000000000000000000000000006'),
                        'soneium',
                        'Token'
                    ),
                    (
                        'WETH',
                        lower('0x4200000000000000000000000000000000000006'),
                        'optimism',
                        'Token'
                    ),
                    (
                        'WETH',
                        lower('0x4200000000000000000000000000000000000006'),
                        'ink',
                        'Token'
                    ),
                    (
                        'WETH',
                        lower('0x82aF49447D8a07e3bd95BD0d56f35241523fBab1'),
                        'arbitrum',
                        'Token'
                    )
            ) as t(symbol, address, chain, category)
    ),

    
    zksync_linea_transfers as (
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
            dim_tokens.address as destination_token,
            origin_chain_id,
            destination_token_symbol,
            destination_chain, 
            source_chain, 
            dim_tokens.category as destination_category
        from across_transfers_chain_mapping
        left join dim_tokens on lower(destination_token_symbol) = lower(symbol)
        where lower(contract_address) in (lower('0xE0B015E54d54fc84a6cB9B666099c46adE9335FF'), lower('0x7E63A5f1a8F0B4d0934B2f2327DAED3F6bb2ee75'))
    ),

    zksync_linea_prices as (
        select hour, token_address, decimals, avg(price) as price
        from ethereum_flipside.price.ez_prices_hourly
        inner join dim_tokens on lower(token_address) = lower(address)
        group by 1, 2, 3
    ),

    zksync_linea_volume_by_chain_and_symbol as (
        select
            date_trunc('hour', block_timestamp) as hour,
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
            destination_chain, 
            source_chain, 
            destination_category,
            coalesce((amount / power(10, p.decimals)) * price, 0) as amount_usd
        from zksync_linea_transfers t
        left join
            zksync_linea_prices p
            on date_trunc('hour', t.block_timestamp) = p.hour
            and t.destination_token = p.token_address
    ),

    non_zksync_linea_volume_by_chain_and_symbol as (
       select
            date_trunc('hour', block_timestamp) as hour,
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
            destination_chain, 
            source_chain, 
            destination_category,
            coalesce((amount / power(10, p.decimals)) * price, 0) as amount_usd
        from across_transfers_chain_mapping t
        left join
            prices p
            on date_trunc('hour', t.block_timestamp) = p.hour
            and t.destination_token = p.token_address
        where
            lower(t.contract_address) not in (lower('0xE0B015E54d54fc84a6cB9B666099c46adE9335FF'), lower('0x7E63A5f1a8F0B4d0934B2f2327DAED3F6bb2ee75'))
    )
    SELECT 
        *
    FROM zksync_linea_volume_by_chain_and_symbol
    union all
    SELECT 
        *
    FROM non_zksync_linea_volume_by_chain_and_symbol