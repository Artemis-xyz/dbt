{{ config(materialized="table", snowflake_warehouse="MEDIUM") }}

with date_spine as (
    select 
        date
    from {{ ref("dim_date_spine") }}
    where date between '2024-11-12' and to_date(sysdate())
)

, morpho_ethereum_wrapper as (
    select
        date(block_timestamp) as date
        , max(user_address) as user_address
        , max(contract_address) as contract_address
        , max_by(balance / 1e18, date) as balance
    from ethereum_flipside.core.fact_token_balances
    where
        lower(contract_address) = lower('0x58D97B57BB95320F9a05dC918Aef65434969c2B2')
        and lower(user_address) = lower('0x9D03bb2092270648d7480049d0E58d2FcF0E5123')
        and date(block_timestamp) >= '2024-11-21'
    group by date
)

, backfilled_wrapper as (
    select
        d.date
        , last_value(user_address ignore nulls) over (
            order by m.date
            rows between unbounded preceding and current row
        ) as user_address
        , last_value(contract_address ignore nulls) over (
            order by m.date
            rows between unbounded preceding and current row
        ) as contract_address
        , last_value(balance ignore nulls) over (
            order by m.date
            rows between unbounded preceding and current row
        ) as balance
    from date_spine d
    left join morpho_ethereum_wrapper m on m.date = d.date
)

, wrapper_change as (
    select
        date
        , user_address
        , contract_address
        , balance
        , lead(balance) over (order by date desc) - balance as wrapper_change
    from backfilled_wrapper
)

select 
    date
    , user_address
    , contract_address
    , balance
    , wrapper_change
from wrapper_change
where date >= '2024-11-21'
order by date desc