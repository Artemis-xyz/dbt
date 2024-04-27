{% macro fact_daily_sleep(chain) %}
    with
        hours_between_transactions as (
            select
                {% if chain == 'solana' %}
                signers[0] AS from_address,
                {% else %}
                from_address,
                {% endif %}
                date_trunc('day', block_timestamp) as raw_date,
                datediff(
                    'hour',
                    lag(block_timestamp, 1) over (
                        partition by from_address, raw_date order by block_timestamp
                    ),
                    block_timestamp
                ) as hours_last_seen
            from {{ chain }}_flipside.core.fact_transactions
            {% if is_incremental() %}
                where block_timestamp >= dateadd(day, -3, to_date(sysdate()))
            {% endif %}
        ),
        max_hours_between_transactions as (
            select from_address, raw_date, max(hours_last_seen) as max_hours_last_seen
            from hours_between_transactions
            group by from_address, raw_date
        ),
        hours_till_since_midnight as (
            select
                {% if chain == 'solana' %}
                signers[0] AS from_address,
                {% else %}
                from_address,
                {% endif %}
                date_trunc('day', block_timestamp) as raw_date,
                dateadd('day', 1, raw_date) as tmw_date,
                min(block_timestamp) as first_day_transactions,
                max(block_timestamp) as last_day_transactions,
                datediff('hour', last_day_transactions, tmw_date) as hour_till_midnight,
                datediff(
                    'hour', raw_date, first_day_transactions
                ) as hour_since_midnight
            from {{ chain }}_flipside.core.fact_transactions
            {% if is_incremental() %}
                where block_timestamp >= dateadd(day, -3, to_date(sysdate()))
            {% endif %}
            group by from_address, raw_date
        )
    select
        max_hours_between_transactions.from_address,
        max_hours_between_transactions.raw_date,
        hours_till_since_midnight.hour_till_midnight,
        hours_till_since_midnight.hour_since_midnight,
        hour_till_midnight + hour_since_midnight as hours_before_after_transactions,
        max_hours_between_transactions.max_hours_last_seen,
        case
            when (hour_till_midnight + hour_since_midnight) >= 7
            then 'MORE THAN 7 HOURS'
            when max_hours_last_seen >= 7
            then 'MORE THAN 7 HOURS'
            else 'LESS THAN 7 HOURS'
        end as user_type
    from max_hours_between_transactions
    left join
        hours_till_since_midnight
        on max_hours_between_transactions.from_address
        = hours_till_since_midnight.from_address
        and max_hours_between_transactions.raw_date = hours_till_since_midnight.raw_date
{% endmacro %}
