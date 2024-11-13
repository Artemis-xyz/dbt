{{
    config(
        materialized='table',
        unique_key=["date"]
    )
}}

with spine as (
  select '2000-01-01'::date as date union all
  select dateadd('day', 1, date) from spine
   where date < '2049-12-31'
),
days (number, name, short_name, shorter_name) as (
    select 1, 'Monday',    'Mon', 'M'  union
    select 2, 'Tuesday',   'Tue', 'Tu' union
    select 3, 'Wednesday', 'Wed', 'W'  union
    select 4, 'Thursday',  'Thu', 'Th' union
    select 5, 'Friday',    'Fri', 'F'  union
    select 6, 'Saturday',  'Sat', 'Sa' union
    select 0, 'Sunday',    'Sun', 'Su'
),
months (number, name, short_name) as (
    select  1, 'January',   'Jan' union
    select  2, 'February',  'Feb' union
    select  3, 'March',     'Mar' union
    select  4, 'April',     'Apr' union
    select  5, 'May',       'May' union
    select  6, 'June',      'Jun' union
    select  7, 'July',      'Jul' union
    select  8, 'August',    'Aug' union
    select  9, 'September', 'Sep' union
    select 10, 'October',   'Oct' union
    select 11, 'November',  'Nov' union
    select 12, 'December',  'Dec'
),
dates as (
  select date,
         date_part('year',  date) as year,
         date_part('month', date) as month,
         date_part('dow',   date) as day_of_week,
         date_part('day',   date) as day_of_month,
         date_part('woy',   date) as week_of_year,

         day_of_week in (0, 6) as is_weekend,
         not is_weekend        as is_weekday,

         round(ceil(month / 3.0), 0) as quarter_of_year,
         round(ceil(month / 6.0), 0) as half_of_year
    from spine
)
select dates.*,

       days.name         as day_of_week_name,
       days.short_name   as day_of_week_short_name,
       days.shorter_name as day_of_week_shorter_name,

       months.name       as month_name,
       months.short_name as month_short_name,

       'Y' || dates.year            as year_text,
       'Q' || dates.quarter_of_year as quarter_of_year_text,
       'H' || dates.half_of_year    as half_of_year_text,

       year_text || quarter_of_year_text as year_and_quarter_text,
       year_text || half_of_year_text    as year_and_half_text
  from dates
  join days
    on dates.day_of_week = days.number
  join months
    on dates.month = months.number