{{ config(materialized="table") }}
with
    flipside_contract as (
        select
            address,
            name,
            namespace,
            null as sub_category,
            category,
            'arbitrum' as chain
        from {{ ref("arb_token_type") }}
        union
        select
            address,
            name,
            namespace,
            null as sub_category,
            category,
            'avalanche' as chain
        from {{ ref("avax_token_type") }}
        union
        select address, name, namespace, null as sub_category, category, 'base' as chain
        from {{ ref("base_token_type") }}
        union
        select address, name, namespace, null as sub_category, category, 'bsc' as chain
        from {{ ref("bsc_token_type") }}
        union
        select
            address,
            name,
            namespace,
            null as sub_category,
            category,
            'ethereum' as chain
        from {{ ref("eth_token_type") }}
        union
        select
            address, name, namespace, null as sub_category, category, 'polygon' as chain
        from {{ ref("polygon_token_type") }}
        union
        select
            address,
            name,
            namespace,
            null as sub_category,
            category,
            'optimism' as chain
        from {{ ref("opt_token_type") }}
        union
        select address, name, namespace, sub_category, category, 'near' as chain
        from {{ ref("dim_flipside_near_contracts") }}
        union
        select address, name, namespace, sub_category, category, 'solana' as chain
        from {{ ref("dim_flipside_solana_contracts") }}
        union 
        select address, name, namespace, sub_category, category, 'sei' as chain
        from {{ ref("dim_flipside_sei_contracts") }}
    )
select
    address,
    chain,
    coalesce(max_by(name, category), max(name)) as name,
    coalesce(max_by(namespace, category), max(namespace)) as namespace,
    max(sub_category) as sub_category,
    max(category) as category
from flipside_contract
group by address, chain
