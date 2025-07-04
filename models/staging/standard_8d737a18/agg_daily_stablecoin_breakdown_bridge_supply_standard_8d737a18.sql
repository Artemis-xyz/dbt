{{ config(materialized="table", snowflake_warehouse="STANDARD_8D737A18") }}

{% set chain_list = ['arbitrum', 'avalanche', 'base', 'bsc', 'celo', 'ethereum', 'mantle', 'optimism', 'polygon', 'solana', 'sui', 'ton', 'tron', 'sonic', 'kaia', 'aptos', 'ripple'] %}

{% set premint_chains = ['aptos', 'avalanche', 'celo', 'ethereum', 'kaia', 'polygon', 'solana', 'sui', 'ton', 'tron'] %}
{% set bridge_chains = ['ethereum', 'tron'] %}

with
    bridge_addresses as (
        {% for chain in bridge_chains %}
            select 
                '{{ chain }}' as chain
                , contract_address
                , premint_address
                , 'bridge_address' as type
            from {{ ref("fact_" ~ chain ~ "_stablecoin_bridge_addresses") }}
            {% if not loop.last %} union all {% endif %}
        {% endfor %}
    )
    , premint_addresses as (
        {% for chain in premint_chains %}
            select 
                '{{ chain }}' as chain
                , contract_address
                , premint_address
                , 'premint_address' as type
            from {{ ref("fact_" ~ chain ~ "_stablecoin_premint_addresses") }}
            {% if not loop.last %} union all {% endif %}
        {% endfor %}
    )

select 
    date
    , t1.chain as chain_name
    , ca.chain_agnostic_id as chain_id
    , case 
        when substr(ca.chain_agnostic_id, 0, 7) = 'eip155:' then lower(ca.chain_agnostic_id || ':' || replace(replace(t1.contract_address, '0x', ''), '0:', '')) 
        when substr(ca.chain_agnostic_id, 0, 11) = 'sui:mainnet' then ca.chain_agnostic_id || ':' || SPLIT_PART(replace(t1.contract_address, '0x', ''), '::', 1) 
        else ca.chain_agnostic_id || ':' || replace(replace(t1.contract_address, '0x', ''), '0:', '') 
    end as asset_id
    , t1.contract_address
    , t1.symbol as asset_symbol
    , sum(
        case 
            when t2.type is null then stablecoin_supply
            else 0
        end
    ) as native_usd
    , sum(
        case 
            when pa.type = 'premint_address' then stablecoin_supply
            else 0
        end
    ) as not_issued_usd
    , sum(
        case 
            when t2.type = 'bridged' then stablecoin_supply
            when ba.type = 'bridge_address' then -1 * stablecoin_supply
            else 0
        end
    ) as bridged_usd
    , native_usd - not_issued_usd + bridged_usd as net_circulation_usd
from {{ ref("agg_daily_stablecoin_balances") }} t1
left join {{ ref("stablecoin_contract_bridged_metadata") }} t2
    on lower(t1.contract_address) = lower(t2.contract_address)
    and t1.chain = t2.chain
left join {{ ref("chain_agnostic_ids") }} ca
    on t1.chain = ca.chain
left join bridge_addresses ba
    on t1.chain = ba.chain
    and lower(t1.address) = lower(ba.premint_address)
left join premint_addresses pa
    on t1.chain = pa.chain
    and lower(t1.address) = lower(pa.premint_address)
    and lower(t1.contract_address) = lower(pa.contract_address)
group by 1, 2, 3, 4, 5, 4

-- native_usd: the amount issued natively on-chain or zero for bridged chains
-- not_issued_usd: the amount held in treasuries
-- bridged_usd: the amount bridged, so negative for native chains and positive for bridged chains
-- net_circulation_usd: native_usd - not_issued_usd + bridged_usd

