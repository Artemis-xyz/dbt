{{ config(materialized="table") }}

with filtered_address as (
    select
        REGEXP_SUBSTR(address, 'sei[^/]+') AS address,
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
            when label_type = 'dapp'
            then 'DeFi'
            when label_type = 'operator'
            then 'Utility'
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
        case
            when label_type = 'dex'
            then 'DEX'
            when label_type = 'cex'
            then 'CEX'
            else null
        end as sub_category,
        modified_timestamp
    from sei_flipside.core.dim_labels as labels
    union all 
    select
        address,
        name,
        lower(
            trim(
                replace(
                    replace(regexp_replace(
                        name,
                        '[^\x00-\x7F]', ''), ' ', '_'
                    ),
                    '-',
                    '_'
                ),
                '_'
            )
        ) as namespace,
        case 
            when name = 'Dragonswap'
            then 'DeFi'
            when symbol is not null and decimals is not null
            then 'Token'
            else null
        end as category,
        null as sub_category,
        modified_timestamp
    from sei_flipside.core_evm.dim_contracts
)
select 
    address,
    'sei' as chain,
    name,
    case 
        when namespace = 'wormhole' and category = 'Bridge' and contains(name, 'usd coin')
        then 'cirlce'
        when namespace = 'wormhole' and category = 'Bridge' and contains(name, 'usdc')
        then 'circle'
        when namespace = 'wormhole' and category = 'Bridge' and contains(name, 'tether usd')
        then 'tether'
        when namespace = 'wormhole' and category = 'Bridge' and contains(name, 'usdt')
        then 'tether'
        else namespace
    end as namespace,
    case 
        when namespace = 'wormhole' and category = 'Bridge' and contains(name, 'usd coin')
        then 'Stablecoin'
        when namespace = 'wormhole' and category = 'Bridge' and contains(name, 'tether usd')
        then 'Stablecoin'
        when namespace = 'wormhole' and category = 'Bridge' and contains(name, 'usdt')
        then 'Stablecoin'
        when namespace = 'webump' or namespace = 'pallet' then 'NFT Apps'
        else category
    end as category,
    sub_category,
    modified_timestamp as last_updated
from filtered_address
