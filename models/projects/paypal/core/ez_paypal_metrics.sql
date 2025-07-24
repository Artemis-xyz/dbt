{{
    config(
        materialized="incremental",
        snowflake_warehouse="PAYPAL",
        database="paypal",
        schema="core",
        alias="ez_metrics",
        incremental_strategy="merge",
        unique_key="date",
        on_schema_change="append_new_columns",
        merge_update_columns=var("backfill_columns", []),
        merge_exclude_columns=["created_on"] | reject('in', var("backfill_columns", [])) | list,
        full_refresh=false,
        tags=["ez_metrics"],
    )
}}

{% set backfill_date = var("backfill_date", None) %}

with paypal as (
    select
    date,
    type,
    transfer_volume,
    transfer_volume * 1e6/ 
    case 
      when extract(year from date) % 4 = 0 
           and (extract(year from date) % 100 != 0 or extract(year from date) % 400 = 0)
      then 366
      else 365
    end as daily_transaction_volume
    from {{ ref("benchmark_seed") }}
    where type = 'PAYPAL'
),
date_boundaries as (
    select 
        min(date) as min_date,
        max(date) as max_date
    from paypal
),

sequence as (
    select seq4() as num
    from table(generator(rowcount => 1e6))  -- Using a constant large number
    where seq4() <= (select datediff(day, min_date, max_date) + 1 from date_boundaries)
),
date_spine as (
    select
        dateadd(day, s.num, d.min_date) as date
    from date_boundaries d
    cross join sequence s
),
-- Get year-end values and prepare for interpolation
year_data as (
    select
        date as year_end,
        lag(date) over (order by date) as prev_year_end,
        daily_transaction_volume as end_volume,
        lag(daily_transaction_volume) over (order by date) as prev_end_volume
    from paypal
),
-- Calculate year boundaries and daily increments
year_metrics as (
    select
        date_trunc('year', year_end) as year_start,
        year_end,
        prev_end_volume,
        end_volume,
        datediff(day, date_trunc('year', year_end), year_end) + 1 as days_in_year,
        (end_volume - prev_end_volume) / nullif(datediff(day, date_trunc('year', year_end), year_end) + 1, 0) as daily_increment
    from year_data
    where prev_year_end is not null
),
-- Final result with smoothly increasing daily volume
final_result as (
    select
        ds.date,
        ym.prev_end_volume + (datediff(day, ym.year_start, ds.date) * ym.daily_increment) as transfer_volume
    from date_spine ds
    join year_metrics ym on ds.date >= ym.year_start and ds.date <= ym.year_end
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
    to_timestamp_ntz(current_timestamp()) as created_on
    , to_timestamp_ntz(current_timestamp()) as modified_on
from combined_result
where true
{{ ez_metrics_incremental('date', backfill_date) }}
and date < to_date(sysdate())
order by date