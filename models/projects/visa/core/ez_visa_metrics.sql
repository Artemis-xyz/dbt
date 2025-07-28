{{
    config(
        materialized="incremental",
        snowflake_warehouse="VISA",
        database="visa",
        schema="core",
        alias="ez_metrics",
        incremental_strategy="merge",
        unique_key="date",
        on_schema_change="append_new_columns",
        merge_update_columns=var("backfill_columns", []),
        merge_exclude_columns=["created_on"] | reject('in', var("backfill_columns", [])) | list,
        full_refresh=false,
        tags=["ez_metrics"]
    )
}}

{% set backfill_date = var("backfill_date", None) %}

with visa as (
    select
        date,
        type,
        transfer_volume,
        transfer_volume * 1e6 / 
        case 
        when extract(month from date) in (1, 4, 7, 10) then 
            case 
            when extract(month from date) = 1 and
                extract(year from date) % 4 = 0 and 
                (extract(year from date) % 100 != 0 or extract(year from date) % 400 = 0)
            then 91
            else 90
            end
        when extract(month from date) in (2, 5, 8, 11) then 91
        when extract(month from date) in (3, 6, 9, 12) then 
            case 
            when extract(month from date) = 3 and
                extract(year from date) % 4 = 0 and 
                (extract(year from date) % 100 != 0 or extract(year from date) % 400 = 0)
            then 92
            else 91
            end
        end as daily_avg_volume
    from {{ ref("benchmark_seed") }}
    where type = 'VISA'
),
date_boundaries as (
    select 
        min(date_trunc('quarter', date)) as min_date,
        max(last_day(dateadd('month', 2, date_trunc('quarter', date)))) as max_date
    from visa
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
-- Process quarters with interpolation information
quarter_info as (
    select
        date,
        date_trunc('quarter', date) as quarter_start,
        last_day(dateadd('month', 2, date_trunc('quarter', date))) as quarter_end,
        daily_avg_volume,
        lag(daily_avg_volume) over (order by date) as prev_quarter_avg,
        row_number() over (order by date) as quarter_num
    from visa
),
-- Final result with desired pattern
final_result as (
    select
        ds.date,
        case
            -- First quarter: flat daily average
            when qi.quarter_num = 1 then
                qi.daily_avg_volume
                
            -- Other quarters: smooth increase from prev quarter avg to current quarter avg
            else
                qi.prev_quarter_avg + 
                (datediff(day, qi.quarter_start, ds.date) * 
                 (qi.daily_avg_volume - qi.prev_quarter_avg) / 
                 datediff(day, qi.quarter_start, qi.quarter_end))
        end as transfer_volume
    from date_spine ds
    join quarter_info qi on ds.date between qi.quarter_start and qi.quarter_end
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