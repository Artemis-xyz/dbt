{% macro ez_rwa_by_product(issuer) %}

select
    tvl.date,
    tvl.issuer,
    tvl.symbol,
    tvl.tokenized_supply_change,
    tvl.tokenized_mcap_change,
    tvl.tokenized_supply,
    tvl.tokenized_mcap,
from {{ ref('fact_' ~ issuer ~ '_tvl_by_product') }} tvl
where date < to_date(sysdate())

{% endmacro %}
