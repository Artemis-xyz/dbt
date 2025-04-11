{% macro get_rolling_active_address_metrics_by_app (app, chain) %}
    with
    {% if chain == 'solana' %}
            distinct_dates as (
                select distinct 
                    raw_date
                from {{ ref("fact_" ~ chain ~ "_transactions_v2") }}
                {% if is_incremental() %}
                    and raw_date > (select dateadd('day', -1, max(date)) from {{ this }})
                {% endif %}
            ),
            distinct_dates_for_rolling_active_address as (
                select distinct 
                    raw_date,
                    value as from_address 
                from {{ ref("fact_" ~ chain ~ "_transactions_v2") }}, lateral flatten(input => signers)
                where succeeded = 'TRUE' and app = '{{ app }}'
            ),
    {% elif chain == 'sui' %}
        distinct_dates as (
            select distinct
                raw_date
            from {{ ref("fact_" ~ chain ~ "_transactions_v2") }}
            {% if is_incremental() %}
                where raw_date > (select dateadd('day', -1, max(date)) from {{ this }})
            {% endif %}
        ),
        distinct_dates_for_rolling_active_address as (
            select distinct
                raw_date,
                sender as from_address
            from {{ ref("fact_" ~ chain ~ "_transactions_v2") }}
            WHERE status = 'success' and app = '{{ app }}'
        ),
    {% elif chain == 'near' %}
        distinct_dates as (
            select distinct
                raw_date
            from {{ ref("fact_" ~ chain ~ "_transactions_v2") }}
            {% if is_incremental() %}
                where raw_date > (select dateadd('day', -1, max(date)) from {{ this }})
            {% endif %}
        ),
        distinct_dates_for_rolling_active_address as (
            select distinct
                raw_date,
                from_address
            from {{ ref("fact_" ~ chain ~ "_transactions_v2") }}
            WHERE app = '{{ app }}' and tx_succeeded = TRUE
        ),
    {% else %}
        distinct_dates as (
            select distinct 
                raw_date
            from {{ ref("fact_" ~ chain ~ "_transactions_v2") }}
            {% if is_incremental() %}
                where raw_date > (select dateadd('day', -1, max(date)) from {{ this }})
            {% endif %}
        ),
        distinct_dates_for_rolling_active_address as (
            select distinct 
                raw_date,
                from_address
            from {{ ref("fact_" ~ chain ~ "_transactions_v2") }}
            where app = '{{ app }}'
        ),
    {% endif %}


    rolling_mau as (
        select 
            t1.raw_date,
            count(distinct t2.from_address) as mau
        from distinct_dates t1
        join distinct_dates_for_rolling_active_address t2 on t2.raw_date between dateadd(DAY, -29, t1.raw_date) and t1.raw_date
        group by t1.raw_date
    ),
    rolling_wau as (
        select 
            t1.raw_date,
            count(distinct t2.from_address) as wau
        from distinct_dates t1
        join distinct_dates_for_rolling_active_address t2 on t2.raw_date between dateadd(DAY, -6, t1.raw_date) and t1.raw_date
        group by t1.raw_date
    )
select 
    rolling_mau.raw_date as date,
    '{{ app }}' as app,
    mau,
    wau
from rolling_mau
left join rolling_wau using(raw_date)
where rolling_mau.raw_date < to_date(sysdate())
order by date
{% endmacro %}
