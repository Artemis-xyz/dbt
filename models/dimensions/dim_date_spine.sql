{{
    config(
        materialized='view',
        unique_key=["date"]
    )
}}

with spine as (
  select '2000-01-01'::date as date union all
  select dateadd('day', 1, date) from spine
   where date < '2049-12-31'
),
days as (
    select * from (values 
        (1, 'Monday',    'Mon', 'M'),
        (2, 'Tuesday',   'Tue', 'Tu'),
        (3, 'Wednesday', 'Wed', 'W'),
        (4, 'Thursday',  'Thu', 'Th'),
        (5, 'Friday',    'Fri', 'F'),
        (6, 'Saturday',  'Sat', 'Sa'),
        (0, 'Sunday',    'Sun', 'Su')
    ) as t(number, name, short_name, shorter_name)
),
months as (
    select * from (values
        (1,  'January',   'Jan'),
        (2,  'February',  'Feb'),
        (3,  'March',     'Mar'),
        (4,  'April',     'Apr'),
        (5,  'May',       'May'),
        (6,  'June',      'Jun'),
        (7,  'July',      'Jul'),
        (8,  'August',    'Aug'),
        (9,  'September', 'Sep'),
        (10, 'October',   'Oct'),
        (11, 'November',  'Nov'),
        (12, 'December',  'Dec')
    ) as t(number, name, short_name)
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