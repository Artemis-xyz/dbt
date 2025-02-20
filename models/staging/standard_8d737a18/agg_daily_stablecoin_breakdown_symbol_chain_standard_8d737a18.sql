{{ config(materialized="incremental", unique_key=["date_day", "asset_id"], snowflake_warehouse="STABLECOIN_V2_LG") }}

select
    date as date_day
    , agg.chain as chain_name
    , ca.chain_agnostic_id as chain_id
    , ca.chain_agnostic_id || ':' || replace(contract_address, '0x', '') as asset_id
    , contract_address
    , symbol as asset_symbol
    , stablecoin_supply as supply_usd
from {{ ref("agg_daily_stablecoin_breakdown_silver") }} agg
left join {{ ref("chain_agnostic_ids") }} ca
    on agg.chain = ca.chain
{% if is_incremental() %}
    where date >= (select max(date_day) from {{ this }})
{% endif %}