{% macro fundamental_data_by_chain(chain) %}
    with
        min_date as (
            select min(block_timestamp) as start_timestamp, from_address
            from prod.fact_{{ chain }}_transactions_gold
            group by from_address
        ),
        new_users as (
            select
                count(distinct from_address) as new_users,
                date_trunc('day', start_timestamp) as start_date
            from min_date
            group by start_date
        ),
        bot as (
            select
                raw_date,
                count(distinct from_address) as low_sleep_users,
                count(*) as tx_n
            from prod.fact_{{ chain }}_transactions_gold
            where user_type = 'LOW_SLEEP'
            group by user_type, raw_date
        ),
        sybil as (
            select
                raw_date,
                engagement_type,
                count(distinct from_address) as sybil_users,
                count(*) as tx_n
            from prod.fact_{{ chain }}_transactions_gold
            where engagement_type = 'sybil'
            group by engagement_type, raw_date
        ),
        chain_agg as (
            select
                raw_date as date,
                max(chain) as chain,
                sum(tx_fee) gas,
                sum(gas_usd) gas_usd,
                count(*) txns,
                count(distinct from_address) daa
            from prod.fact_{{ chain }}_transactions_gold
            group by date
        )
        {% if (chain not in ("near", "base")) %}
            ,
            users_over_100 as (
                select
                    count(distinct from_address) as dau_over_100,
                    raw_date as balance_date
                from prod.fact_{{ chain }}_transactions_gold
                where balance_usd >= 100
                group by raw_date
            )
        {% endif %}
    select
        date,
        chain,
        txns,
        daa,
        gas,
        gas_usd,
        (daa - new_users) as returning_users,
        new_users,
        low_sleep_users,
        (daa - low_sleep_users) as high_sleep_users,
        sybil_users,
        (daa - sybil_users) as non_sybil_users
        {% if (chain not in ("near", "base")) %}, dau_over_100
        {% else %}, null as dau_over_100
        {% endif %}
    from chain_agg
    left join new_users on date = new_users.start_date
    left join bot on date = bot.raw_date
    left join sybil on date = sybil.raw_date
    {% if (chain not in ("near", "base")) %}
        left join users_over_100 on date = users_over_100.balance_date
    {% endif %}
{% endmacro %}
