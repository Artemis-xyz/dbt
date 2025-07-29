{{
    config(
        materialized="incremental",
        snowflake_warehouse="FEDWIRE",
        database="fedwire",
        schema="core",
        alias="ez_metrics",
        incremental_strategy="merge",
        unique_key="date",
        on_schema_change="append_new_columns",
        merge_update_columns=var("backfill_columns", []),
        merge_exclude_columns=["created_on"] if not var("backfill_columns", []) else none,
        full_refresh=false,
        tags=["ez_metrics"],
    )
}}

{% set backfill_date = var("backfill_date", None) %}

with fedwire as (
    select
    date,
    type,
    transfer_volume * 1e6 as daily_transaction_volume
    from {{ ref("benchmark_seed") }}
    where type = 'FEDWIRE'
),
date_boundaries as (
    select 
        min(date_trunc('month', date)) as min_date,
        max(last_day(date)) as max_date
    from fedwire
),
sequence as (
    select seq4() as num
    from table(generator(rowcount => 1e6))
    where seq4() <= (select datediff(day, min_date, max_date) + 1 from date_boundaries)
),
date_spine as (
    select
        dateadd(day, s.num, d.min_date) as date
    from date_boundaries d
    cross join sequence s
),
-- Process months with interpolation information
month_info as (
    select
        date,
        date_trunc('month', date) as month_start,
        last_day(date) as month_end,
        daily_transaction_volume,
        lag(daily_transaction_volume) over (order by date) as prev_month_avg,
        row_number() over (order by date) as month_num
    from fedwire
),
-- Final result with desired pattern
final_result as (
    select
        ds.date,
        case
            -- First month: flat daily average
            when mi.month_num = 1 then
                mi.daily_transaction_volume
                
            -- Other months: smooth increase from prev month avg to current month avg
            else
                mi.prev_month_avg + 
                (datediff(day, mi.month_start, ds.date) * 
                 (mi.daily_transaction_volume - mi.prev_month_avg) / 
                 datediff(day, mi.month_start, mi.month_end))
        end as transfer_volume
    from date_spine ds
    join month_info mi on ds.date between mi.month_start and mi.month_end
),
-- Get the last date and value from original calculations
last_data_point as (
    select max(date) as last_date
    from final_result
),
last_value as (
    select transfer_volume
    from final_result
    where date = (select last_date from last_data_point)
),
-- Extended date boundaries to include today
extended_date_boundaries as (
    select 
        (select max(date) + 1 from final_result) as min_date,
        current_date() as max_date
),
-- Create sequence for extrapolated date range
extrapolation_sequence as (
    select seq4() as num
    from table(generator(rowcount => 1e6))
    where seq4() <= (select datediff(day, min_date, max_date) from extended_date_boundaries)
),
-- Create date spine for extrapolated dates
extrapolated_spine as (
    select
        dateadd(day, s.num, d.min_date) as date
    from extended_date_boundaries d
    cross join extrapolation_sequence s
),
-- Create extrapolated values (forward-fill)
extrapolated_result as (
    select
        date,
        (select transfer_volume from last_value) as transfer_volume
    from extrapolated_spine
),
-- Combine original and extrapolated data
combined_result as (
    select date, transfer_volume from final_result
    union all
    select date, transfer_volume from extrapolated_result
)
-- Final output
select 
    date, 
    transfer_volume,
    -- timestamp columns
    TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as created_on,
    TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as modified_on
from combined_result
where true
{{ ez_metrics_incremental('date', backfill_date) }}
and date < to_date(sysdate())
order by date