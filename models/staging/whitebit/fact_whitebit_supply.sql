{{
    config(
        materialized="table"
    )
}}

with supply_data as (
    select
        date(date) as date
        , private_sale
        , burn_address
        , whitebit_launchpad
        , whitebit_funds
    from {{ source('MANUAL_STATIC_TABLES', 'whitebit_supply_data') }}
)

, date_spine as (
    select
        date
    from {{ ref("dim_date_spine") }}
    where date >= (select min(date) from supply_data)
    and date < to_date(sysdate())
)

, joined as (
    select
        ds.date
        , sd.private_sale
        , sd.burn_address
        , sd.whitebit_launchpad
        , sd.whitebit_funds AS foundation_owned_supply_native
    from date_spine ds
    left join supply_data sd on ds.date = sd.date
)

, forward_filled as (
    select
        date
        , SUM(private_sale) OVER (ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as cumulative_private_sale
        , SUM(burn_address) OVER (ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as cumulative_burns_native
        , SUM(whitebit_launchpad) OVER (ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as cumulative_whitebit_launchpad
        , SUM(foundation_owned_supply_native) OVER (ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as cumulative_foundation_owned_supply_native
    from joined
)

select 
    date
    , cumulative_private_sale
    , cumulative_whitebit_launchpad
    , cumulative_foundation_owned_supply_native
    , cumulative_burns_native
    , 400000000 AS max_supply_native
    , 400000000 - cumulative_burns_native - cumulative_private_sale - cumulative_whitebit_launchpad - cumulative_foundation_owned_supply_native as total_unvested_supply_native
from forward_filled
