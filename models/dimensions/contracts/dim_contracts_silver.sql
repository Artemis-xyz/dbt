{{ config(materialized="table") }}

with
    contracts as (
        select lower(address) address, chain
        from {{ ref("dim_dune_contracts_post_sigma") }}
        union
        -- add near + sei 
        select lower(address) address, chain
        from {{ ref("dim_flipside_contracts") }}
        where chain in ('near', 'sei')
        union
        -- add arbitrum, avalanche, base, bsc, ethereum, polygon, optimism contracts that aren't token contracts
        select lower(address) address, chain
        from {{ ref("dim_flipside_contracts") }}
        where chain in ('arbitrum', 'avalanche', 'base', 'bsc', 'ethereum', 'polygon', 'optimism')
            and category not in ('NFT', 'Token', 'ERC_1155')
        union
        -- add solana contracts that aren't token contracts
        select lower(address) address, chain
        from {{ ref("dim_flipside_contracts") }}
        where
            chain = 'solana'
            and (
                sub_category <> 'nf_token_contract' and sub_category <> 'token_contract'
            )
        union
        select lower(address) address, chain
        from {{ ref("dim_usersubmittedcontracts") }}
        union
        select lower(address) address, chain
        from {{ ref("dim_scanner_contracts") }}
        union 
        select lower(address) address, chain
        from {{ ref("dim_sui_contracts") }}
    ),
    distinct_contract as (select address, chain from contracts group by address, chain),
    contract_waterfall as (
        select
            dc.address address,
            dc.chain chain,
            coalesce(
                user_sub.namespace,
                scanner.namespace,
                dune.namespace,
                sui.namespace,
                flipside_near.namespace,
                flipside_sol.namespace,
                flipside_sei.namespace,
                flipside_arbitrum.namespace,
                flipside_avalanche.namespace,
                flipside_base.namespace,
                flipside_bsc.namespace,
                flipside_ethereum.namespace,
                flipside_polygon.namespace,
                flipside_optimism.namespace,
                null
            ) namespace,
            coalesce(
                user_sub.name,
                scanner.name,
                dune.name,
                sui.name,
                flipside_near.name,
                flipside_sol.name,
                flipside_sei.name,
                flipside_arbitrum.name,
                flipside_avalanche.name,
                flipside_base.name,
                flipside_bsc.name,
                flipside_ethereum.name,
                flipside_polygon.name,
                flipside_optimism.name,
                null
            ) as name
        from distinct_contract dc
        left join
            {{ ref("dim_usersubmittedcontracts") }} as user_sub
            on lower(dc.address) = lower(user_sub.address)
            and dc.chain = user_sub.chain
        left join
            {{ ref("dim_dune_contracts_post_sigma") }} as dune
            on lower(dc.address) = lower(dune.address)
            and dc.chain = dune.chain
        left join
            {{ ref("dim_flipside_contracts") }} as flipside_near
            on lower(dc.address) = lower(flipside_near.address)
            and dc.chain = flipside_near.chain
            and flipside_near.chain = 'near'
        left join
            {{ ref("dim_flipside_contracts") }} as flipside_sei
            on lower(dc.address) = lower(flipside_sei.address)
            and dc.chain = flipside_sei.chain
            and flipside_sei.chain = 'sei'
        left join
            {{ ref("dim_flipside_contracts") }} as flipside_arbitrum
            on lower(dc.address) = lower(flipside_arbitrum.address)
            and dc.chain = flipside_arbitrum.chain
            and flipside_arbitrum.chain = 'arbitrum'
            and flipside_arbitrum.category not in ('NFT', 'Token', 'ERC_1155')
        left join
            {{ ref("dim_flipside_contracts") }} as flipside_avalanche
            on lower(dc.address) = lower(flipside_avalanche.address)
            and dc.chain = flipside_avalanche.chain
            and flipside_avalanche.chain = 'avalanche'
            and flipside_avalanche.category not in ('NFT', 'Token', 'ERC_1155')
        left join
            {{ ref("dim_flipside_contracts") }} as flipside_base
            on lower(dc.address) = lower(flipside_base.address)
            and dc.chain = flipside_base.chain
            and flipside_base.chain = 'base'
            and flipside_base.category not in ('NFT', 'Token', 'ERC_1155')
        left join
            {{ ref("dim_flipside_contracts") }} as flipside_bsc
            on lower(dc.address) = lower(flipside_bsc.address)
            and dc.chain = flipside_bsc.chain
            and flipside_bsc.chain = 'bsc'
            and flipside_bsc.category not in ('NFT', 'Token', 'ERC_1155')
        left join
            {{ ref("dim_flipside_contracts") }} as flipside_ethereum
            on lower(dc.address) = lower(flipside_ethereum.address)
            and dc.chain = flipside_ethereum.chain
            and flipside_ethereum.chain = 'ethereum'
            and flipside_ethereum.category not in ('NFT', 'Token', 'ERC_1155')
        left join
            {{ ref("dim_flipside_contracts") }} as flipside_polygon
            on lower(dc.address) = lower(flipside_polygon.address)
            and dc.chain = flipside_polygon.chain
            and flipside_polygon.chain = 'polygon'
            and flipside_polygon.category not in ('NFT', 'Token', 'ERC_1155')
        left join
            {{ ref("dim_flipside_contracts") }} as flipside_optimism
            on lower(dc.address) = lower(flipside_optimism.address)
            and dc.chain = flipside_optimism.chain
            and flipside_optimism.chain = 'optimism'
            and flipside_optimism.category not in ('NFT', 'Token', 'ERC_1155')
        left join
            {{ ref("dim_flipside_contracts") }} as flipside_sol
            on lower(dc.address) = lower(flipside_sol.address)
            and dc.chain = flipside_sol.chain
            and flipside_sol.chain = 'solana'
            and (
                flipside_sol.sub_category <> 'nf_token_contract'
                and flipside_sol.sub_category <> 'token_contract'
            )
        left join
            {{ ref("dim_scanner_contracts") }} as scanner
            on lower(dc.address) = lower(scanner.address)
            and dc.chain = scanner.chain
        left join {{ ref("dim_sui_contracts") }} sui
            on lower(dc.address) = lower(sui.address)
            and dc.chain = sui.chain
    ),
    contracts_to_parent_app as (
        select
            contract_waterfall.address,
            contract_waterfall.name as name,
            coalesce(apps.parent_app, apps.namespace) as app,
            contract_waterfall.chain
        from contract_waterfall
        left join
            {{ ref("dim_apps_gold") }} apps
            on contract_waterfall.namespace = apps.namespace
    ),
    contracts_to_parent_labels as (
        select
            contracts.address,
            contracts.chain,
            contracts.name,
            contracts.app,
            parent_app.category as category,
            parent_app.sub_category as sub_category,
            parent_app.friendly_name as friendly_name
        from contracts_to_parent_app as contracts
        left join
            {{ ref("dim_apps_gold") }} parent_app
            on contracts.app = parent_app.namespace
    ),
    contracts_to_apps as (
        select
            address,
            coalesce(max_by(name, app), max(name)) as name,
            max(app) as app,
            max_by(friendly_name, app) as friendly_name,
            chain,
            max_by(category, app) as category,
            max_by(sub_category, app) as sub_category
        from contracts_to_parent_labels
        group by address, chain
    ),
    -- get token contracts. Prevents tokens from getting counted as usage of apps
    flipside_token_type as (
        select address, name, chain, category
        from {{ ref("dim_flipside_contracts") }}
        where
            chain not in (
                'near', 
                'solana', 
                'sei', 
                'arbitrum',
                'avalanche',
                'base',
                'bsc',
                'ethereum',
                'polygon',
                'optimism'
            ) and category in ('NFT', 'Token', 'ERC_1155')
        union
        -- Explicitly select the chains that have token contracts
        select address, name, chain, category
        from {{ ref("dim_flipside_contracts") }}
        where
            chain in (
                'arbitrum',
                'avalanche',
                'base',
                'bsc',
                'ethereum',
                'polygon',
                'optimism'
            ) and category in ('NFT', 'Token', 'ERC_1155')
        union
        select address, name, chain, category
        from {{ ref("dim_flipside_contracts") }}
        where
            chain = 'solana'
            and category in ('NFT', 'Token')
            and sub_category in ('nf_token_contract', 'token_contract')
    ),
    contracts_labeled as (
        select
            contracts_to_apps.friendly_name as friendly_name,
            coalesce(contracts_to_apps.address, token_type.address) as address,
            coalesce(contracts_to_apps.name, token_type.name) as name,
            contracts_to_apps.app,
            coalesce(contracts_to_apps.chain, token_type.chain) as chain,
            coalesce(contracts_to_apps.category, token_type.category) as category,
            contracts_to_apps.sub_category
        from contracts_to_apps
        full join
            flipside_token_type as token_type
            on lower(contracts_to_apps.address) = lower(token_type.address)
            and contracts_to_apps.chain = token_type.chain
    )
select
    address,
    max_by(name, address) as name,
    max(app) as app,
    max_by(friendly_name, app) as friendly_name,
    chain,
    max_by(category, app) as category,
    max_by(sub_category, app) as sub_category
from contracts_labeled
group by address, chain
