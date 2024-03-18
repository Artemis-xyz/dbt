{% macro fact_daily_uniswap_fork_unique_traders(
    token_address, chain, _event_name, _key, app
) %}
    with
        pools as (
            select decoded_log:{{ _key }}::string as pool
            from {{ chain }}_flipside.core.ez_decoded_event_logs
            where
                contract_address = lower('{{ token_address }}')
                and event_name = '{{ _event_name }}'
        ),
        all_pool_events as (
            select
                t1.block_number,
                trunc(t1.block_timestamp, 'day') as date,
                t1.origin_from_address as sender
            from {{ chain }}_flipside.core.ez_decoded_event_logs t1
            inner join pools t2 on lower(t1.contract_address) = lower(t2.pool)
            where
                t1.event_name in ('Swap') and sender not in (select pool from pools)
                {% if is_incremental() %}
                    and trunc(t1.block_timestamp, 'day')
                    >= (select max(date) + interval '1 DAY' from {{ this }})

                {% endif %}
                and trunc(t1.block_timestamp, 'day')
                < trunc(convert_timezone('UTC', sysdate()), 'day')
        ),
        unique_traders_daily as (
            select
                date,
                '{{ chain }}' as chain,
                '{{ app }}' as app,
                'DeFi' as category,
                count(distinct sender) as unique_traders
            from all_pool_events
            where date is not null
            group by date
        )
    select date, chain, app, category, unique_traders
    from unique_traders_daily
    where date is not null
{% endmacro %}
