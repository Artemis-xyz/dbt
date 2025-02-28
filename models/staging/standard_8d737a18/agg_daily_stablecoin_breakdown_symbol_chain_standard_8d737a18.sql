{{ config(materialized="incremental", unique_key=["date_day", "asset_id"], snowflake_warehouse="STABLECOIN_V2_LG") }}

select
    date as date_day
    , agg.chain as chain_name
    , ca.chain_agnostic_id as chain_id
    , case 
        when substr(ca.chain_agnostic_id, 0, 7) = 'eip155:' then lower(ca.chain_agnostic_id || ':' || replace(replace(contract_address, '0x', ''), '0:', '')) 
        when substr(ca.chain_agnostic_id, 0, 11) = 'sui:mainnet' then ca.chain_agnostic_id || ':' || SPLIT_PART(replace(contract_address, '0x', ''), '::', 1) 
        else ca.chain_agnostic_id || ':' || replace(replace(contract_address, '0x', ''), '0:', '') 
    end as asset_id
    , contract_address
    , symbol as asset_symbol
    , sum(stablecoin_supply) as supply_usd
from {{ ref("agg_daily_stablecoin_breakdown_silver") }} agg
left join {{ ref("chain_agnostic_ids") }} ca
    on agg.chain = ca.chain
{% if is_incremental() %}
    where date >= (select dateadd('day', -7, max(date)) from {{ this }})
{% endif %}
group by date_day, chain_name, chain_id, asset_id, contract_address, asset_symbol




