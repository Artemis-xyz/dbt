{% macro get_eth_l2_cost(addresses, start_date) %}
    select block_timestamp::date as date, sum(gas_used * gas_price) / 1e9 as gas
    from ethereum_flipside.core.fact_transactions
    where
        -- https://l2beat.com/scaling/projects/scroll#permissions
        lower(to_address) in (
            {% for addr in addresses %}
            lower('{{ addr }}') {% if not loop.last %},{% endif %}
            {% endfor %}
        )
        {% if is_incremental() %}
            and block_timestamp >= dateadd(day, -5, (select min(date) from {{ this }}))
        {% endif %}
        and block_timestamp < date(sysdate())
        and block_timestamp >= '{{ start_date }}'
    group by 1
{% endmacro %}