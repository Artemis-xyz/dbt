{{
    config(
        materialized="incremental",
        unique_key=["date"],
        snowflake_warehouse="STACKS",
    )
}}

with 
    distinct_dates as (
        select distinct 
            block_timestamp::date as raw_date
        from {{ ref("fact_stacks_transactions") }}
        where tx_status = 'success'
        and block_timestamp::date >= '2024-07-10'
    ),
    distinct_dates_for_rolling_active_address as (
        select distinct 
            block_timestamp::date raw_date,
            sender_address as from_address 
        from {{ ref("fact_stacks_transactions") }}
        where tx_status = 'success'
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
        'stack' as chain,
        mau,
        wau
    from rolling_mau
    left join rolling_wau using(raw_date)
    where rolling_mau.raw_date < to_date(sysdate())  
    order by date
