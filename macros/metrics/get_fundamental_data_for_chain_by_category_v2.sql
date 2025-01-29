{% macro get_fundamental_data_for_chain_by_category_v2(chain) %}
    select 
        max(date) as date,
        category,
        max(chain) as chain,
        sum(gas) as gas,
        sum(gas_usd) as gas_usd,
        sum(txns) as txns,
        sum(dau) as dau,
        sum(returning_users) as returning_users,
        sum(new_users) as new_users,
        sum(low_sleep_users) as low_sleep_users,
        sum(high_sleep_users) as high_sleep_users,
        sum(sybil_users) as sybil_users,
        sum(non_sybil_users) as non_sybil_users
    from {{ chain }}.prod_core.ez_metrics_by_subcategory
    group by category
{% endmacro %}
