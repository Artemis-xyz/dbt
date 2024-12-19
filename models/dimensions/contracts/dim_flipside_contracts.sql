{{ config(materialized="table") }}
with
    flipside_contract as (
        select address, name, namespace, sub_category, category, 'arbitrum' as chain, last_updated
        from {{ ref("dim_flipside_arbitrum_contracts") }}

        union
        select address, name, namespace, sub_category, category, 'avalanche' as chain, last_updated
        from {{ ref("dim_flipside_avalanche_contracts") }}

        union
        select address, name, namespace, sub_category, category, 'base' as chain, last_updated
        from {{ ref("dim_flipside_base_contracts") }}

        union
        select address, name, namespace, sub_category, category, 'bsc' as chain, last_updated
        from {{ ref("dim_flipside_bsc_contracts") }}

        union
        select address, name, namespace, sub_category, category, 'ethereum' as chain, last_updated
        from {{ ref("dim_flipside_ethereum_contracts") }}

        union
        select address, name, namespace, sub_category, category, 'polygon' as chain, last_updated
        from {{ ref("dim_flipside_polygon_contracts") }}

        union
        select address, name, namespace, sub_category, category, 'optimism' as chain, last_updated
        from {{ ref("dim_flipside_optimism_contracts") }}

        union
        select address, name, namespace, sub_category, category, 'near' as chain, last_updated
        from {{ ref("dim_flipside_near_contracts") }}

        union
        select address, name, namespace, sub_category, category, 'solana' as chain, last_updated
        from {{ ref("dim_flipside_solana_contracts") }}

        union 
        select address, name, namespace, sub_category, category, 'sei' as chain, last_updated
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
