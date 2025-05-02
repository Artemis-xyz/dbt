{{ config(materialized="incremental", unique_key="date", tags=["sui"]) }}

{% set max_date = get_max_timestamp(this, "date") %}

with
    distinct_dates as (
        select distinct raw_date
        from {{ source("PROD_LANDING", "fact_sui_transactions_silver_bigquery_v2") }}
        {% if is_incremental() %}
            where raw_date >= date_sub(date('{{ max_date }}'), interval 1 day)
        {% endif %}
    ),

    distinct_dates_for_rolling_active_address as (
        select distinct raw_date, from_address
        from {{ source("PROD_LANDING", "fact_sui_transactions_silver_bigquery_v2") }}
        {% if is_incremental() %}
            where raw_date >= date_sub(date('{{ max_date }}'), interval 29 day)
        {% endif %}
    ),

    rolling_mau as (
        select t1.raw_date, count(distinct t2.from_address) as mau
        from distinct_dates t1
        join
            distinct_dates_for_rolling_active_address t2
            on t2.raw_date
            between date_sub(t1.raw_date, interval 29 day) and t1.raw_date
        group by t1.raw_date
    ),

    rolling_wau as (
        select t1.raw_date, count(distinct t2.from_address) as wau
        from distinct_dates t1
        join
            distinct_dates_for_rolling_active_address t2
            on t2.raw_date between date_sub(t1.raw_date, interval 6 day) and t1.raw_date
        group by t1.raw_date
    )

select rolling_mau.raw_date as date, 'sui' as chain, mau, wau
from rolling_mau
left join rolling_wau using (raw_date)
where rolling_mau.raw_date < current_date()
order by date
