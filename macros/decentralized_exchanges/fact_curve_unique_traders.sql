{% macro fact_curve_unique_traders(_chain) %}
    with
        tokens as (
            select pool_address
            from {{ ref("dim_curve_pools_gold") }}
            {% if _chain == "avalanche" %} where chain = 'avax'
            {% else %} where chain = '{{ _chain }}'
            {% endif %}
        ),
        events as (
            select trunc(block_timestamp, 'day') as date, origin_from_address
            from {{ _chain }}_flipside.core.ez_decoded_event_logs t1
            inner join tokens t2 on lower(t1.contract_address) = lower(t2.pool_address)
            where
                event_name in ('TokenExchange', 'TokenExchangeUnderlying')
                {% if is_incremental() %}
                    and trunc(t1.block_timestamp, 'day')
                    >= (select max(date) + interval '1 DAY' from {{ this }})

                {% endif %}
        )
    select
        date,
        '{{ _chain }}' as chain,
        'curve' as app,
        'DeFi' as category,
        count(distinct origin_from_address) as unique_traders
    from events
    where date < to_date(sysdate())
    group by date
{% endmacro %}
