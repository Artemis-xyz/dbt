{% macro get_fundamental_data_for_chain_by_application(chain, model_version='') %}
{% set model_name = "fact_" ~ chain ~ "_transactions" ~ ("_v2" if model_version == "v2" else "") %}
    with
        min_date as (
            select min(raw_date) as start_date, from_address, app
            from {{ ref(model_name) }}
            where not equal_null(category, 'EOA') and app is not null
            and raw_date < to_date(sysdate())
            group by app, from_address
        ),
        new_users as (
            select count(distinct from_address) as new_users, app, start_date
            from min_date
            group by start_date, app
        ),
        bot as (
            select
                raw_date,
                app,
                count(distinct from_address) as low_sleep_users,
                count(*) as tx_n
            from {{ ref(model_name) }}
            where user_type = 'LOW_SLEEP' and app is not null
            and raw_date < to_date(sysdate())
            group by user_type, raw_date, app
        ),
        sybil as (
            select
                raw_date,
                app,
                count(distinct from_address) as sybil_users,
                count(*) as tx_n
            from {{ ref(model_name) }}
            where engagement_type = 'sybil'
            and raw_date < to_date(sysdate())
            group by engagement_type, raw_date, app
        ),
        agg_data as (
            select
                raw_date,
                app,
                max(chain) as chain,
                max(friendly_name) friendly_name,
                max(category) category,
                max(sub_category) as sub_category,
                sum(tx_fee) gas,
                sum(gas_usd) gas_usd,
                count(*) txns,
                count(distinct from_address) dau
            from {{ ref(model_name) }}
            where not equal_null(category, 'EOA') and app is not null
            and raw_date < to_date(sysdate())
            group by raw_date, app
        )
    select
        agg_data.raw_date as date,
        agg_data.app,
        chain,
        friendly_name,
        category,
        sub_category,
        gas,
        gas_usd,
        txns,
        dau,
        (dau - new_users) as returning_users,
        new_users,
        low_sleep_users,
        (dau - low_sleep_users) as high_sleep_users,
        sybil_users,
        (dau - sybil_users) as non_sybil_users
    from agg_data
    left join
        new_users
        on equal_null(agg_data.app, new_users.app)
        and agg_data.raw_date = new_users.start_date
    left join
        bot on equal_null(agg_data.app, bot.app) and agg_data.raw_date = bot.raw_date
    left join
        sybil
        on equal_null(agg_data.app, sybil.app)
        and agg_data.raw_date = sybil.raw_date
{% endmacro %}
