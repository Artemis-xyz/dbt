{{ config(materialized="table") }}

with
    flipside_namespace as (
        -- Arbitrum Flipside Filtered Namespaces
        select
            namespace,
            initcap(replace(replace(namespace, '-', ' '), '_', ' ')) as friendly_name,
            max(category) as category,
            max(sub_category) as sub_category
        from {{ ref("dim_flipside_contracts") }}
        where chain = 'arbitrum' and namespace is not null
            and category not in ('NFT', 'Token', 'ERC_1155')
        group by namespace
        
        union all
        -- Avalanche Flipside Filtered Namespaces
        select
            namespace,
            initcap(replace(replace(namespace, '-', ' '), '_', ' ')) as friendly_name,
            max(category) as category,
            max(sub_category) as sub_category
        from {{ ref("dim_flipside_contracts") }}
        where chain = 'avalanche' and namespace is not null
            and category not in ('NFT', 'Token', 'ERC_1155')
        group by namespace

        union all
        -- Base Flipside Filtered Namespaces
        select
            namespace,
            initcap(replace(replace(namespace, '-', ' '), '_', ' ')) as friendly_name,
            max(category) as category,
            max(sub_category) as sub_category
        from {{ ref("dim_flipside_contracts") }}
        where chain = 'base' and namespace is not null
            and category not in ('NFT', 'Token', 'ERC_1155')
        group by namespace

        union all
        -- BSC Flipside Filtered Namespaces
        select
            namespace,
            initcap(replace(replace(namespace, '-', ' '), '_', ' ')) as friendly_name,
            max(category) as category,
            max(sub_category) as sub_category
        from {{ ref("dim_flipside_contracts") }}
        where chain = 'bsc' and namespace is not null
            and category not in ('NFT', 'Token', 'ERC_1155')
        group by namespace

        union all
        -- Ethereum Flipside Filtered Namespaces
        select
            namespace,
            initcap(replace(replace(namespace, '-', ' '), '_', ' ')) as friendly_name,
            max(category) as category,
            max(sub_category) as sub_category
        from {{ ref("dim_flipside_contracts") }}
        where chain = 'ethereum' and namespace is not null
            and category not in ('NFT', 'Token', 'ERC_1155')
        group by namespace

        union all
        -- Polygon Flipside Filtered Namespaces
        select
            namespace,
            initcap(replace(replace(namespace, '-', ' '), '_', ' ')) as friendly_name,
            max(category) as category,
            max(sub_category) as sub_category
        from {{ ref("dim_flipside_contracts") }}
        where chain = 'polygon' and namespace is not null
            and category not in ('NFT', 'Token', 'ERC_1155')
        group by namespace

        union all
        -- Optimism Flipside Filtered Namespaces
        select
            namespace,
            initcap(replace(replace(namespace, '-', ' '), '_', ' ')) as friendly_name,
            max(category) as category,
            max(sub_category) as sub_category
        from {{ ref("dim_flipside_contracts") }}
        where chain = 'optimism' and namespace is not null
            and category not in ('NFT', 'Token', 'ERC_1155')
        group by namespace

        union all
        -- Sei Flipside Filtered Namespaces
        select
            namespace,
            initcap(replace(replace(namespace, '-', ' '), '_', ' ')) as friendly_name,
            max(category) as category,
            max(sub_category) as sub_category
        from {{ ref("dim_flipside_contracts") }}
        where chain = 'sei' and namespace is not null
        group by namespace

        union all
        -- NEAR Flipside Filtered Namespaces
        select
            namespace,
            initcap(replace(replace(namespace, '-', ' '), '_', ' ')) as friendly_name,
            max(category) as category,
            max(sub_category) as sub_category
        from {{ ref("dim_flipside_contracts") }}
        where chain = 'near' and namespace is not null
        group by namespace

        union all

        -- Solana Flipside Filtered Namespaces
        select
            namespace,
            initcap(replace(replace(namespace, '-', ' '), '_', ' ')) as friendly_name,
            max(category) as category,
            null as sub_category
        from {{ ref("dim_flipside_contracts") }}
        where
            chain = 'solana'
            and namespace is not null
            and sub_category <> 'nf_token_contract'
            and sub_category <> 'token_contract'
        group by namespace
    )
select
    namespace,
    coalesce(max_by(friendly_name, namespace), max(friendly_name)) as friendly_name,
    coalesce(max_by(category, namespace), max(category)) as category,
    coalesce(max_by(sub_category, namespace), max(sub_category)) as sub_category
from flipside_namespace
group by namespace
