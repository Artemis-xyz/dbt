{% macro current_balances(chain) %}
    select
        max_by(balance_token, block_timestamp) as token_balance,
        max(block_timestamp) as block_timestamp,
        address,
        contract_address
    from {% if chain in ('ton') %} ton.prod_core.ez_ton_address_balances_by_token {% else %} prod.fact_{{ chain }}_address_balances_by_token {% endif %}
    {% if is_incremental() %}
        where block_timestamp >= DATEADD('day', -3, to_date(sysdate()))
    {% endif %}
    group by address, contract_address
{% endmacro %}
