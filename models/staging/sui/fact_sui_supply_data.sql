{{
    config(
        materialized="table",
        snowflake_warehouse="SUI",
    )
}}

with supply_data as (
    select
        date(date) as date
        , max_supply AS max_supply_native
        , total_supply AS total_supply_native
        , foundation_owned_supply AS foundation_owned_supply_native
        , unvested_tokens AS unvested_tokens_native
    from {{ source('MANUAL_STATIC_TABLES', 'sui_daily_supply_data') }}
)

, date_spine as (
    select
        date
    from {{ ref("dim_date_spine") }}
    where date between 
        (select min(date) from supply_data)
        and 
        (select max(date) from supply_data)
)

, joined as (
    select
        ds.date
        , sd.max_supply_native
        , sd.total_supply_native
        , sd.foundation_owned_supply_native
        , sd.unvested_tokens_native
    from date_spine ds
    left join supply_data sd on ds.date = sd.date
)

, forward_filled as (
    select
        date
        , last_value(total_supply_native ignore nulls) over (
            order by date
            rows between unbounded preceding and current row
        ) as total_supply_native
        , last_value(foundation_owned_supply_native ignore nulls) over (
            order by date
            rows between unbounded preceding and current row
        ) as foundation_owned_supply_native
        , last_value(unvested_tokens_native ignore nulls) over (
            order by date
            rows between unbounded preceding and current row
        ) as unvested_tokens_native
        , last_value(max_supply_native ignore nulls) over (
            order by date
            rows between unbounded preceding and current row
        ) as max_supply_native
    from joined
)

select 
    date
    , max_supply_native
    , total_supply_native
    , foundation_owned_supply_native
    , unvested_tokens_native
from forward_filled
