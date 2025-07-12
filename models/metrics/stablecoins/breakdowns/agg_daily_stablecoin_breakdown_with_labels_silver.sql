{{ config(materialized="incremental", unique_key="unique_id", snowflake_warehouse="STABLECOIN_V2_LG") }}

{% set chain_list = ['arbitrum', 'avalanche', 'base', 'bsc', 'celo', 'ethereum', 'mantle', 'optimism', 'polygon', 'solana', 'sui', 'ton', 'tron', 'sonic', 'kaia', 'aptos', 'ripple', 'stellar', 'sei'] %}
{% set list_stablecoin_address = var('list_stablecoin_address', []) %}
{% set stablecoin_models = [] %}
{% for chain in chain_list %}
    {% do stablecoin_models.append(ref("ez_" ~ chain ~ "_stablecoin_metrics_by_address_with_labels")) %}
    {% do stablecoin_models.append(ref("fact_" ~ chain ~ "_stablecoin_contracts")) %}
{% endfor %}

with
    daily_data as (
    {% if is_incremental() and list_stablecoin_address | length > 0 %}
        -- If specific addresses exist, check if they exist in fact_<chain>_stablecoin_contracts before querying
        {% for chain in chain_list %}
            select *
            from {{ ref("ez_" ~ chain ~ "_stablecoin_metrics_by_address_with_labels") }}
            where lower (contract_address) IN (
                select lower(contract_address)
                from {{ ref("fact_" ~ chain ~ "_stablecoin_contracts") }}
                where lower(contract_address) IN (
                    {% for address in list_stablecoin_address %}
                        lower('{{ address }}'){% if not loop.last %}, {% endif %}
                    {% endfor %}
                )
            )
            {% if not loop.last %} union all {% endif %}
        {% endfor %}
    {% endif %}
    {% if list_stablecoin_address | length == 0 %}
        {% for chain in chain_list %}
            select *
            from {{ ref("ez_" ~ chain ~ "_stablecoin_metrics_by_address_with_labels") }}
            {% if is_incremental() %}
                where date >= (select DATEADD('day', -3, max(date)) from {{ this }})
            {% endif %}
            {% if not loop.last %} union all {% endif %}
        {% endfor %}
    {% endif %}
)

select
    date,
    address,
    name, 
    friendly_name,
    icon,
    artemis_application_id,
    INITCAP(REPLACE(artemis_application_id, '_', ' ')) as application,
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
from daily_data
where date < to_date(sysdate())