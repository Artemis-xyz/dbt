
{{ 
    config(
        snowflake_warehouse="COMMON",
        database="common",
        schema="core",
        materialized='view'
    )
}}

select 
   date,
    address,
    name, 
    friendly_name,
    icon,
    artemis_application_id,
    application,
    artemis_category_id,
    
    is_wallet,

    contract_address,
    symbol,
    
    stablecoin_transfer_volume,
    stablecoin_daily_txns,
    artemis_stablecoin_transfer_volume,
    artemis_stablecoin_daily_txns,
    p2p_stablecoin_transfer_volume,
    p2p_stablecoin_daily_txns,
    stablecoin_supply,
    chain,
    unique_id
from {{ ref('agg_daily_stablecoin_breakdown_with_labels_silver') }}