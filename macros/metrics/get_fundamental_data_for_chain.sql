{% macro get_fundamental_data_for_chain(chain) %}
    with
        min_date as (
            select min(block_timestamp) as start_timestamp, from_address
            from {{ chain }}.prod_raw.ez_transactions
            group by from_address
        ),
        new_users as (
            select
                count(distinct from_address) as new_users,
                date_trunc('day', start_timestamp) as start_date
            from min_date
            group by start_date
        ),
        {% if chain not in ("starknet") %}
            bot as (
                select
                    raw_date,
                    count(distinct from_address) as low_sleep_users,
                    count(*) as tx_n
                from {{ chain }}.prod_raw.ez_transactions
                where user_type = 'LOW_SLEEP'
                group by user_type, raw_date
            ),
            sybil as (
                select
                    raw_date,
                    engagement_type,
                    count(distinct from_address) as sybil_users,
                    count(*) as tx_n
                from {{ chain }}.prod_raw.ez_transactions
                where engagement_type = 'sybil'
                group by engagement_type, raw_date
            ),
        {% endif %}
        chain_agg as (
            select
                raw_date as date,
                max(chain) as chain,
                {% if chain not in ("starknet") %} --Starknet allows for multiple types of tokens to be used for gas
                    sum(tx_fee) fees_native,
                {% else %}
                    null as fees_native,
                {% endif %}
                sum(gas_usd) fees,
                count(*) txns,
                sum(gas_usd) / count(*) as avg_txn_fee,
                count(distinct from_address) dau
            from {{ chain }}.prod_raw.ez_transactions
            group by date
        )
        {% if (chain not in ("near", "starknet")) %}
            ,
            users_over_100 as (
                select
                    count(distinct from_address) as dau_over_100,
                    raw_date as balance_date
                from {{ chain }}.prod_raw.ez_transactions
                where balance_usd >= 100
                group by raw_date
            )
        {% endif %}
    select
        date,
        chain,
        txns,
        dau,
        fees_native,
        fees,
        avg_txn_fee,
        (dau - new_users) as returning_users,
        new_users,
        {% if (chain not in ("starknet")) %}
            low_sleep_users,
            (dau - low_sleep_users) as high_sleep_users,
            sybil_users,
            (dau - sybil_users) as non_sybil_users
        {% else %}
            null as low_sleep_users,
            null as high_sleep_users,
            null as sybil_users,
            null as non_sybil_users
        {% endif %}
        {% if (chain not in ("near", "starknet")) %}, dau_over_100
        {% else %}, null as dau_over_100
        {% endif %}
    from chain_agg
    left join new_users on date = new_users.start_date
    {% if chain not in ("starknet") %}
        left join bot on date = bot.raw_date
        left join sybil on date = sybil.raw_date
    {% endif %}
    {% if (chain not in ("near", "starknet")) %}
        left join users_over_100 on date = users_over_100.balance_date
    {% endif %}
{% endmacro %}
