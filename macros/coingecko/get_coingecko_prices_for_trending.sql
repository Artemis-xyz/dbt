{% macro get_coingecko_price_for_trending(coingecko_id) %}
    select date as date, shifted_token_price_usd as price
    from {{ ref("fact_coingecko_token_date_adjusted_gold") }}
    where
        coingecko_id = '{{ coingecko_id }}'
        and date < dateadd(day, -1, to_date(sysdate()))
    union
    select dateadd('day', -1, to_date(sysdate())) as date, token_current_price as price
    from {{ ref("fact_coingecko_token_realtime_data") }}
    where token_id = '{{ coingecko_id }}'
    union
    select to_date(sysdate()) as date, token_current_price as price
    from {{ ref("fact_coingecko_token_realtime_data") }}
    where token_id = '{{ coingecko_id }}'
    order by date desc
{% endmacro %}
