{% macro get_nft_metrics(chain) %}
    select date, nft_trading_volume
    from {{ ref("fact_" ~ chain ~ "_nft_trading_volume") }}
{% endmacro %}
