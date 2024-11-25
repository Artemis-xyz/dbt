{% macro ez_rwa_by_product(issuer_id) %}

select
    tvl.date,
    tvl.issuer_friendly_name,
    tvl.symbol,
    sum(tvl.tokenized_supply_change) as tokenized_supply_change,
    sum(tvl.tokenized_mcap_change) as tokenized_mcap_change,
    sum(tvl.tokenized_supply) as tokenized_supply,
    sum(tvl.tokenized_mcap) as tokenized_mcap,
from {{ ref('fact_' ~ issuer_id ~ '_tvl_by_product') }} tvl
where date < to_date(sysdate())
group by 1, 2, 3
{% endmacro %}
