{{ config(materialized="incremental", unique_key=["date_day", "asset_id"], snowflake_warehouse="STABLECOIN_V2_LG") }}

{% set chain_list = ['arbitrum', 'avalanche', 'base', 'bsc', 'celo', 'ethereum', 'mantle', 'optimism', 'polygon', 'solana', 'sui', 'ton', 'tron', 'kaia', 'aptos'] %}


with
    stablecoin_metrics as (
        {% for chain in chain_list %}
            select *
            from {{ ref("ez_" ~ chain ~ "_stablecoin_metrics_by_address_with_labels")}}
            {% if is_incremental() %}
                where date >= (select DATEADD('day', -3, max(date)) from {{ this }})
            {% endif %}
            {% if not loop.last %}
                union all
            {% endif %}
        {% endfor %}
    )

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
from stablecoin_metrics agg
left join {{ ref("chain_agnostic_ids") }} ca
    on agg.chain = ca.chain
where symbol in ('USDT', 'USDC', 'USDe', 'USDS', 'DAI', 'USDtb', 'FDUSD', 'PYUSD', 'USD0', 'TUSD', 'RLUSD', 'BUSD', 'EURC', 'cUSD')-- TODO: Need to keep a list of assets to include not remove
{% if is_incremental() %}
    and date >= (select DATEADD('day', -3, max(date_day)) from {{ this }})
{% endif %}
group by date_day, chain_name, chain_id, asset_id, contract_address, asset_symbol