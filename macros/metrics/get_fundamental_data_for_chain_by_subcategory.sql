{% macro get_fundamental_data_for_chain_by_subcategory(chain, model_version='') %}
{% set model_name = "fact_" ~ chain ~ "_transactions" ~ ("_v2" if model_version == "v2" else "") %}
    with
        min_date as (
            select min(raw_date) as start_date, from_address, category, sub_category
            from {{ ref(model_name) }}
            where raw_date < to_date(sysdate())
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
            from {{ ref(model_name) }}
            where user_type = 'LOW_SLEEP'
            and raw_date < to_date(sysdate())
            group by user_type, raw_date, category, sub_category
        ),
        sybil as (
            select
                raw_date,
                category,
                sub_category,
                count(distinct from_address) as sybil_users,
                count(*) as tx_n
            from {{ ref(model_name) }}
            where engagement_type = 'sybil'
            and raw_date < to_date(sysdate())
            group by engagement_type, raw_date, category, sub_category
        ),
        real_users as (
            select
                -- Define a grouping key depending on whether app is null
                COALESCE(app, contract_address) AS group_key,
                from_address
            from {{ ref(model_name) }}
                where raw_date < to_date(sysdate())
                group by from_address, group_key
                having count(*) >= 2 and sum(gas_usd) > 0.0001
            ),
        agg_data as (
            select
                raw_date,
                m.category,
                m.sub_category,
                max(chain) as chain,
                sum(tx_fee) gas,
                sum(gas_usd) gas_usd,
                count(*) txns,
                count(distinct m.from_address) dau,
                count(distinct contract_address) contract_count,
                count(distinct ru.from_address) real_users
            from {{ ref(model_name) }} m
            left join real_users ru
                on m.from_address = ru.from_address
                and coalesce(m.app, m.contract_address) = ru.group_key
            where raw_date < to_date(sysdate())
            group by raw_date, m.category, m.sub_category
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
        contract_count,
        real_users,
        (dau - coalesce(new_users, 0)) as returning_users,
        coalesce(new_users, 0) as new_users,
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
