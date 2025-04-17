{% macro get_fundamental_data_for_chain_by_category(chain, model_version='') %}
{% set model_name = "fact_" ~ chain ~ "_transactions" ~ ("_v2" if model_version == "v2" else "") %}
    with
        min_date as (
            select min(raw_date) as start_date, from_address, category
            from {{ ref(model_name) }}
            group by category, from_address
        ),
        new_users as (
            select count(distinct from_address) as new_users, category, start_date
            from min_date
            group by start_date, category
        ),
        bot as (
            select
                raw_date,
                category,
                count(distinct from_address) as low_sleep_users,
                count(*) as tx_n
            from {{ ref(model_name) }}
            where user_type = 'LOW_SLEEP'
            group by user_type, raw_date, category
        ),
        sybil as (
            select
                raw_date,
                category,
                count(distinct from_address) as sybil_users,
                count(*) as tx_n
            from {{ ref(model_name) }}
            where engagement_type = 'sybil'
            group by engagement_type, raw_date, category
        ),
        agg_data as (
            select
                raw_date,
                category,
                max(chain) as chain,
                sum(tx_fee) gas,
                sum(gas_usd) gas_usd,
                count(*) txns,
                count(distinct from_address) dau
            from {{ ref(model_name) }}
            group by raw_date, category
        )
    select
        agg_data.raw_date as date,
        ifnull(agg_data.category, 'Unlabeled') as category,
        chain,
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
        on equal_null(agg_data.category, new_users.category)
        and agg_data.raw_date = new_users.start_date
    left join
        bot
        on equal_null(agg_data.category, bot.category)
        and agg_data.raw_date = bot.raw_date
    left join
        sybil
        on equal_null(agg_data.category, sybil.category)
        and agg_data.raw_date = sybil.raw_date
{% endmacro %}
