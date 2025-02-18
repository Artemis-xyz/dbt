{% macro get_fundamental_data_for_chain_by_subcategory(chain, model_version='') %}
    with
        min_date as (
            select min(raw_date) as start_date, from_address, category, sub_category
            from {{ chain }}.prod_raw.ez_transactions{% if model_version == 'v2' %}_v2{% endif %}
            group by sub_category, category, from_address
        ),
        new_users as (
            select count(distinct from_address) as new_users, category, sub_category, start_date
            from min_date
            group by start_date, category, sub_category
        ),
        bot as (
            select
                raw_date,
                category,
                sub_category,
                count(distinct from_address) as low_sleep_users,
                count(*) as tx_n
            from {{ chain }}.prod_raw.ez_transactions{% if model_version == 'v2' %}_v2{% endif %}
            where user_type = 'LOW_SLEEP'
            group by user_type, raw_date, category, sub_category
        ),
        sybil as (
            select
                raw_date,
                category,
                sub_category,
                count(distinct from_address) as sybil_users,
                count(*) as tx_n
            from {{ chain }}.prod_raw.ez_transactions{% if model_version == 'v2' %}_v2{% endif %}
            where engagement_type = 'sybil'
            group by engagement_type, raw_date, category, sub_category
        ),
        agg_data as (
            select
                raw_date,
                category,
                sub_category,
                max(chain) as chain,
                sum(tx_fee) gas,
                sum(gas_usd) gas_usd,
                count(*) txns,
                count(distinct from_address) dau
            from {{ chain }}.prod_raw.ez_transactions{% if model_version == 'v2' %}_v2{% endif %}
            group by raw_date, category, sub_category
        )
    select
        agg_data.raw_date as date,
        ifnull(agg_data.category, 'Unlabeled') as category,
        ifnull(agg_data.sub_category, 'Unlabeled') as sub_category,
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
        and equal_null(agg_data.sub_category, new_users.sub_category)
        and agg_data.raw_date = new_users.start_date
    left join
        bot
        on equal_null(agg_data.category, bot.category)
        and equal_null(agg_data.sub_category, bot.sub_category)
        and agg_data.raw_date = bot.raw_date
    left join
        sybil
        on equal_null(agg_data.category, sybil.category)
        and equal_null(agg_data.sub_category, sybil.sub_category)
        and agg_data.raw_date = sybil.raw_date
{% endmacro %}
