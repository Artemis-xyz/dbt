{% macro current_balances(chain) %}
    select
        max_by(balance_token, block_timestamp) as token_balance,
        max(block_timestamp) as block_timestamp,
        address,
        contract_address
    from prod.fact_{{ chain }}_address_balances_by_token
    {% if is_incremental() %}
        where block_timestamp >= dateadd('day', -7, to_date(sysdate()))
    {% endif %}
    group by address, contract_address
{% endmacro %}
