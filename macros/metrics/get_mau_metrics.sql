{% macro get_mau_metrics(chain) %}
    {% if chain == "tron" %}
        select 
            date_trunc('month', block_timestamp) as date,
            count(distinct from_address) as mau
        from tron_allium.raw.transactions 
        where receipt_status = 1
        group by date
    {% elif chain == "celo" %}
        select 
            date_trunc('month', block_timestamp) as date,
            count(distinct from_address) as mau
        from {{ ref("fact_celo_transactions") }}
        where status = 1
        group by date
    {% else %} 
        select
            date_trunc('month', block_timestamp) as date,
            count(distinct from_address) as mau
        from {{ chain }}_flipside.core.fact_transactions
        where status = 'SUCCESS'
        group by date
    {% endif %}
{% endmacro %}