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

    prices as (
        {{ get_coingecko_prices_on_chains(['ethereum', 'optimism', 'arbitrum', 'polygon', 'base', 'ink', 'soneium', 'linea', 'worldchain', 'unichain', 'zksync']) }}
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
        case
            when contains(lower(destination_token_symbol), 'usd') then 'Stablecoin'
            when contains(lower(destination_token_symbol), 'eth') then 'Token'
        else destination_category end as destination_category,
        coalesce((amount / power(10, p.decimals)) * price, 0) as amount_usd
    from across_transfers_chain_mapping t
    left join
        prices p
        on date_trunc('day', t.block_timestamp) = p.date
        and lower(t.destination_token) = lower(p.contract_address)
