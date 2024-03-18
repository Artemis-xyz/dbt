{{ config(materialized="table") }}

with
    solana_labels as (
        select
            address,
            blockchain as chain,
            address_name as name,
            lower(
                trim(
                    replace(
                        replace(regexp_replace(label, '[^\x00-\x7F]', ''), ' ', '_'),
                        '-',
                        '_'
                    ),
                    '_'
                )
            ) as namespace,
            case
                when label_type = 'cex'
                then 'CeFi'
                when label_type = 'dex'
                then 'DeFi'
                when label_type = 'games'
                then 'Gaming'
                when label_type = 'token'
                then 'Token'
                when label_type = 'defi'
                then 'DeFi'
                when label_type = 'layer2'
                then 'Layer 2'
                when label_type = 'nft'
                then 'NFT'
                when label_type = 'bridge'
                then 'Bridge'
                else null
            end as category,
            label_subtype as sub_category
        from solana_flipside.core.dim_labels as labels
    ),
    manual_filter as (
        select
            address,
            chain,
            name,
            namespace,
            case
                when namespace = 'wormhole' then 'Bridge' else category
            end as category,
            sub_category
        from solana_labels
        where namespace is not null and namespace <> ''
    )
select address, chain, name, namespace, category, sub_category
from manual_filter
