{% macro get_coingecko_price_with_latest(coingecko_id) %}
    select date as date, shifted_token_price_usd as price
    from {{ ref("fact_coingecko_token_date_adjusted_gold") }}
    where
        coingecko_id = '{{ coingecko_id }}'
        and date < dateadd(day, -1, to_date(sysdate()))
    union
    select dateadd('day', -1, to_date(sysdate())) as date, token_current_price as price
    from {{ ref("fact_coingecko_token_realtime_data") }}
    where token_id = '{{ coingecko_id }}'
{% endmacro %}



{% macro get_multiple_coingecko_price_with_latest(chain) %}
    select date as date, contract_address, decimals, symbol, shifted_token_price_usd as price
    from {{ ref("fact_coingecko_token_date_adjusted_gold") }}
    inner join {{ ref('dim_coingecko_token_map')}}
        on coingecko_id = coingecko_token_id
    where
        chain = '{{ chain }}'
        and date < dateadd(day, -1, to_date(sysdate()))
    union
    select dateadd('day', -1, to_date(sysdate())) as date, contract_address, decimals, symbol, token_current_price as price
    from {{ ref("fact_coingecko_token_realtime_data") }}
    inner join {{ ref('dim_coingecko_token_map')}}
        on token_id = coingecko_token_id
    where chain = '{{ chain }}'
{% endmacro %}

{% macro get_coingecko_prices_on_chains(chains) %}
    select date as date, contract_address, decimals, symbol, shifted_token_price_usd as price
    from {{ ref("fact_coingecko_token_date_adjusted_gold") }}
    inner join {{ ref('dim_coingecko_token_map')}}
        on coingecko_id = coingecko_token_id
    where
        chain in  (
            {% for chain in chains %}
                '{{chain}}' {% if not loop.last %},{% endif %}
            {% endfor %}
        )
        and date < dateadd(day, -1, to_date(sysdate()))
    union
    select dateadd('day', -1, to_date(sysdate())) as date, contract_address, decimals, symbol, token_current_price as price
    from {{ ref("fact_coingecko_token_realtime_data") }}
    inner join {{ ref('dim_coingecko_token_map')}}
        on token_id = coingecko_token_id
    where
        chain in  (
            {% for chain in chains %}
                '{{chain}}' {% if not loop.last %},{% endif %}
            {% endfor %}
        )
    union
    select to_date(sysdate()) as date, contract_address, decimals, symbol, token_current_price as price
    from {{ ref("fact_coingecko_token_realtime_data") }}
    inner join {{ ref('dim_coingecko_token_map')}}
        on token_id = coingecko_token_id
    where
        chain in  (
            {% for chain in chains %}
                '{{chain}}' {% if not loop.last %},{% endif %}
            {% endfor %}
        )
{% endmacro %}