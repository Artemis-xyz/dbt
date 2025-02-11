{% macro chainanalysis_stablecoin_transfers(chain, new_stablecoin_address="") %}

{% set chain_name = '' %}
{% if chain == 'celo' %}
    {% set chain_name = 'eip155:42220' %}
{% elif chain == 'solana' %}
    {% set chain_name = 'solana:5eykt4UsFv8P8NJdTREpY1vzqKqZKvdp' %}
{% elif chain == 'ton' %}
    {% set chain_name = 'tvm:-239' %}
{% endif %}

select
    block_timestamp as transaction_timestamp,
    date as date_day,
    block_number,
    index as transfer_index,
    tx_hash as transaction_hash,
    from_address as sender_address,
    to_address as receiver_address,
    is_mint,
    is_burn,
    amount as amount_asset,
    inflow,
    transfer_volume,
    {% if chain == 'solana' %}
        '{{chain_name}}' || contract_address as asset_id,
    {% else %}
        '{{chain_name}}' || replace(contract_address, '0x', '') as asset_id,
    {% endif %}
    symbol as asset_symbol,
    '{{chain_name}}' as chain_name
from {{ ref( "fact_"~ chain ~ "_stablecoin_transfers") }}
{% if is_incremental() and new_stablecoin_address == '' %} 
    where block_timestamp >= (
        select dateadd('day', -3, max(block_timestamp))
        from {{ this }}
    )
{% endif %}
{% if new_stablecoin_address != '' %}
    where lower(contract_address) = lower('{{ new_stablecoin_address }}')
{% endif %}

{% endmacro %}
