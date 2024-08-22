{% macro avg_l7d_coingecko_price(coingecko_id) %}    
    select
        avg(shifted_token_price_usd) as avg_price_7d
    from {{ref("fact_coingecko_token_date_adjusted_gold")}}
    where coingecko_id = '{{ coingecko_id }}'
        and date >= dateadd('day', -7,  to_date(sysdate())::date)
{% endmacro %}
