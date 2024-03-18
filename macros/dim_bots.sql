{% macro dim_bots(chain, fact_daily_bot_model) %}

    with
        sleep_daily_count as (
            select
                count_if(user_type = 'LESS THAN 7 HOURS') as no_sleep_count,
                from_address
            from prod.{{ fact_daily_bot_model }}
            group by from_address
        ),
        from_address_agg_metrics as (
            select
                from_address,
                max(block_timestamp) as latest_transaction_timestamp,
                min(block_timestamp) as first_transaction_timestamp,
                count(*) as cur_total_txns,
                count(distinct to_address) as cur_distinct_to_address_count
            from {{ chain }}_flipside.core.fact_transactions
            group by from_address
        )
    select
        agg_metrics.from_address,
        no_sleep_count,
        first_transaction_timestamp,
        case
            when no_sleep_count >= 5 then 'LOW_SLEEP' else 'HIGH_SLEEP'
        end as user_type,
        datediff(
            'day', first_transaction_timestamp, latest_transaction_timestamp
        ) as address_life_span,
        cur_total_txns,
        cur_distinct_to_address_count
    from from_address_agg_metrics as agg_metrics
    left join
        sleep_daily_count on agg_metrics.from_address = sleep_daily_count.from_address

{% endmacro %}
