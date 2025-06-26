{{ config(materialized="table", snowflake_warehouse="ANALYTICS_XL") }}


select 
    date
    , t1.chain as chain_name
    , ca.chain_agnostic_id as chain_id
    , case 
        when substr(ca.chain_agnostic_id, 0, 7) = 'eip155:' then lower(ca.chain_agnostic_id || ':' || replace(replace(t1.contract_address, '0x', ''), '0:', '')) 
        when substr(ca.chain_agnostic_id, 0, 11) = 'sui:mainnet' then ca.chain_agnostic_id || ':' || SPLIT_PART(replace(t1.contract_address, '0x', ''), '::', 1) 
        else ca.chain_agnostic_id || ':' || replace(replace(t1.contract_address, '0x', ''), '0:', '') 
    end as asset_id
    , t1.symbol as asset_symbol
    , sum(
        case 
            when t2.type is null then stablecoin_supply
            else 0
        end
    ) as native_supply
    , sum(
        case 
            when t2.type = 'bridged' then stablecoin_supply
            else 0
        end
    ) as bridged_supply
from {{ ref("agg_daily_stablecoin_breakdown_with_labels_silver") }} t1
left join {{ ref("stablecoin_contract_bridged_metadata") }} t2
    on lower(t1.contract_address) = lower(t2.contract_address)
    and t1.chain = t2.chain
left join {{ ref("chain_agnostic_ids") }} ca
    on t1.chain = ca.chain
group by 1,2,3,4,5
