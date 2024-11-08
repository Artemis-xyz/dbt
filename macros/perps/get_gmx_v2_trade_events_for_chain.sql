{% macro get_gmx_v2_trade_events_for_chain(chain) %}

{% if chain == 'arbitrum' %}
{% set contract_address = '0xc8ee91a54287db53897056e12d9819156d3822fb' %}
{% elif chain == 'avalanche' %}
{% set contract_address = '0xDb17B211c34240B014ab6d61d4A31FA0C0e20c26' %}
{% endif %}

SELECT
    block_timestamp,
    tx_hash,
    event_index,   
    '{{ chain }}' as chain,
    decoded_log:eventData[1][0][7][1] as execution_price,
    decoded_log:eventData[0][0][1][1]::string as market
FROM
    {{ source((chain | upper) ~ '_FLIPSIDE', 'ez_decoded_event_logs') }}
WHERE
    contract_address = lower('{{contract_address}}')
    and decoded_log:eventName in ('PositionIncrease', 'PositionDecrease')
    {% if is_incremental() %}
        and block_timestamp >= dateadd('day', -1, (select max(block_timestamp) from {{ this }}))
    {% endif %}

{% endmacro %}
