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
            input_token,
            input_symbol,
            t2.chain as destination_chain, 
            t3.chain as source_chain, 
            null as destination_category
        from {{ ref("fact_across_transfers") }} t1
        left join {{ ref("dim_chain_ids")}} t2 on t1.destination_chain_id = t2.id
        left join {{ ref("dim_chain_ids")}} t3 on t1.origin_chain_id = t3.id
    ),
    chain_prices as (
        {{ get_coingecko_prices_on_chains(['ethereum', 'optimism', 'arbitrum', 'polygon', 'base', 'ink', 'soneium', 'linea', 'worldchain', 'unichain', 'zksync']) }}
    ),
    prices as (
        select
            date,
            contract_address,
            max(decimals) as decimals,
            max(symbol) as symbol,
            max(price) as price
        from chain_prices
        group by contract_address, date
    )
    select
        date_trunc('day', block_timestamp) as date,
        version,
        t.contract_address,
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
        input_token as source_token,
        input_symbol as source_token_symbol,
        case
            when contains(lower(destination_token_symbol), 'usd') then 'Stablecoin'
            when contains(lower(destination_token_symbol), 'eth') then 'Token'
        else null end as destination_category,
        coalesce((amount / pow(10, p.decimals)) * price, 0) as amount_usd
    from across_transfers_chain_mapping t
    left join
        prices p
        on date_trunc('day', t.block_timestamp) = p.date
        and lower(t.destination_token) = lower(p.contract_address)
