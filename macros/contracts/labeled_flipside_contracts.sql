{% macro labeled_flipside_contracts(chain, token_type_identifier) %}

with
    chain_labels as (
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
        from ethereum_flipside.core.dim_labels
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
        from chain_labels
        where namespace is not null and namespace <> ''
    ),
    token_filter as (
        select
            address
            , '{{ chain }}' as chain
            , name
            , namespace
            , category
            , null as sub_category
        from {{ ref( token_type_identifier ~ "_token_type")}}
        union 
        select
            address
            , chain
            , name
            , namespace
            , category
            , sub_category
        from manual_filter
        where address not in (select address from {{ ref( token_type_identifier ~ "_token_type")}})
    )
select address, chain, name, namespace, category, sub_category
from token_filter

{% endmacro %}
