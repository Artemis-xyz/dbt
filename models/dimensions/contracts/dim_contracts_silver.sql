{{ config(materialized="table") }}

with
    contracts as (
        select lower(address) address, chain
        from {{ ref("dim_dune_contracts_post_sigma") }}
        union
        -- add near + sei contracts
        select lower(address) address, chain
        from {{ ref("dim_flipside_contracts") }}
        where chain in ('near', 'sei')
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
            chain not in ('near', 'solana', 'sei') and category in ('NFT', 'Token', 'ERC_1155')
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
