{% macro get_bam_data_for_application(app, chains) %}
with 
    fundamental_data_by_chain as (
        SELECT
            chain, 
            date,
            app,
            friendly_name,
            txns,
            gas, 
            gas_usd,
            daa,
            new_users,
            returning_users
        FROM {{ref("fact_daily_bam_datahub_v2")}}
        WHERE app = '{{app}}' 
        and chain in  (
            {% for chain in chains %}
                '{{chain}}' {% if not loop.last %},{% endif %}
            {% endfor %}
        )
        {% if is_incremental() %}
            and date > (select dateadd('day', -3, max(date)) from {{ this }})
        {% endif %}
    )
SELECT
    date,
    app,
    friendly_name,
    sum(gas) as gas,
    sum(gas_usd) as gas_usd,
    sum(txns) as txns,
    sum(daa) as daa,
    sum(new_users) as new_users,
    sum(returning_users) as returning_users
FROM 
    fundamental_data_by_chain
GROUP BY 
    date,
    app,
    friendly_name
{% endmacro %}
