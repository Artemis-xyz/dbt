{{ config(materialized="incremental", unique_key="unique_id", snowflake_warehouse="STABLECOIN_V2_LG") }}
{% set list_stablecoin_address = var('list_stablecoin_address', []) %}
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
from {{ ref("agg_daily_stablecoin_breakdown_with_labels_silver") }}
{% if is_incremental() and list_stablecoin_address | length > 0 %}
    where (
    {% for stablecoin in list_stablecoin_address %}
        {% if not loop.first %}or {% endif %}lower(contract_address) = lower('{{ stablecoin }}')
    {% endfor %}
    )
{% endif %}
{% if is_incremental()  and list_stablecoin_address | length == 0 %}
    where date >= (select DATEADD('day', -3, max(date)) from {{ this }})
{% endif %}