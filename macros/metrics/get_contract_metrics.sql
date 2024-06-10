{% macro get_contract_metrics(chain) %}
    select
        date,
        contract_deployers as weekly_contract_deployers,
        contracts_deployed as weekly_contracts_deployed
    from {{ ref("fact_" ~ chain ~ "_contracts_gold") }}
{% endmacro %}
