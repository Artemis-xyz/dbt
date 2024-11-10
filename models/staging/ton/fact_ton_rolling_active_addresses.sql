{{
    config(
        materialized="incremental",
        unique_key=["date"],
        snowflake_warehouse="TON",
    )
}}

with 
    distinct_dates as (
        select distinct 
            block_timestamp::date as raw_date
        from {{ ref("fact_ton_transactions") }}
        where success
        {% if is_incremental() %}
            and block_timestamp::date >= (select dateadd('day', -3, max(date)) from {{ this }})
        {% endif %}
    ),
    flatten_ton_transactions as (
        select
            NOT ARRAY_CONTAINS(0, ARRAY_UNIQUE_AGG(success)) as success
            , min_by(transaction_account_interface, lt) as interface
            , min_by(transaction_account, lt) as first_account
            , max(block_timestamp::date) as raw_date
        from {{ ref("fact_ton_transactions") }}
        where success and block_timestamp::date >= dateadd(DAY, -29, (select min(raw_date) from distinct_dates))
        group by trace_id
    ),
    distinct_dates_for_rolling_active_address as (
        select distinct 
            raw_date,
            first_account as from_address 
        from flatten_ton_transactions
        where success and interface like '%wallet%'
    ),
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
        'ton' as chain,
        mau,
        wau
    from rolling_mau
    left join rolling_wau using(raw_date)
    where rolling_mau.raw_date < to_date(sysdate())
    order by date
