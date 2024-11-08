{% macro get_gmx_v1_price_per_trade_for_chain(chain) %}

{% if chain == 'arbitrum' %}
{% set contract_address = '0x489ee077994B6658eAfA855C308275EAd8097C4A' %}
{% elif chain == 'avalanche' %}
{% set contract_address = '0x9ab2De34A33fB459b538c43f251eB825645e8595' %}
{% endif %}

with gmx_vault_events as (
    select
        *
    from {{ source((chain | upper) ~ '_FLIPSIDE', 'ez_decoded_event_logs') }}
    where contract_address = lower('{{ contract_address }}')
    {% if is_incremental() %}
        and block_timestamp > (select dateadd('day', -1, max(block_timestamp)) from {{ this }})
    {% endif %}
)
select
    block_timestamp,
    tx_hash,
    event_index,
    '{{ chain }}' as chain,
    decoded_log:indexToken::string as token_address,
    decoded_log:price::number / 1e30 as price
from
    gmx_vault_events
where event_name = 'IncreasePosition'

UNION ALL

select
    block_timestamp,
    tx_hash,
    event_index,
    '{{ chain }}' as chain,
    decoded_log:indexToken::string as token_address,
    decoded_log:price::number / 1e30 as price
from
    gmx_vault_events
where event_name = 'DecreasePosition'

UNION ALL

select
    block_timestamp,
    tx_hash,
    event_index,
    '{{ chain }}' as chain,
    decoded_log:indexToken::string as token_address,
    decoded_log:markPrice::number / 1e30 as price
from
    gmx_vault_events
where event_name = 'LiquidatePosition'


{% endmacro %}
