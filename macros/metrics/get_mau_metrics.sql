{% macro get_mau_metrics(chain) %}
select
    date_trunc('month', block_timestamp) as month,
    count(distinct from_address) as monthly_active_addresses
from {{ chain }}_flipside.core.fact_transactions
where status = 'SUCCESS'
group by month
{% endmacro %}