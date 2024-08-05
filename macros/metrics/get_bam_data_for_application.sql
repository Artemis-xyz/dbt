{% macro get_bam_data_for_application(app, chains) %}
with 
{% if is in ('near', chains) %}
    min_date as (
        select min(raw_date) as start_date, from_address_adjusted
        FROM {{ref("ez_near_transactions")}}
        where app = '{{app}}'
        {% if is_incremental() %}
            and date > (select dateadd('day', -3, max(date)) from {{ this }})
        {% endif %}
        group by app, from_address_adjusted
    ),
    new_users as (
        select count(distinct from_address_adjusted) as new_users, start_date
        from min_date
        group by start_date
    ),
    near_data_by_chain as (
        SELECT
            date,
            app,
            friendly_name,
            count(*) as txns,
            sum(tx_fee) as gas, 
            sum(gas_usd) as gas_usd,
            count(distinct from_address_adjusted) as daa
        FROM {{ref("ez_near_transactions")}}
        WHERE app = '{{app}}' 
        {% if is_incremental() %}
            and date > (select dateadd('day', -3, max(date)) from {{ this }})
        {% endif %}
        GROUP BY 
            date,
            app,
            friendly_name
    ),
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
            daa - new_users as returning_users
        FROM near_data_by_chain
        LEFT JOIN new_users on near_data_by_chain.date = new_users.start_date
    )
{% else %}
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
        FROM {{ref("fact_daily_bam_datahub")}}
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
{% endif %}

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
