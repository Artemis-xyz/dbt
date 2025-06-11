{{ config(materialized="table", snowflake_warehouse="MEDIUM") }}


with date_spine as (
    select 
        date
    from {{ ref("dim_date_spine") }}
    where date between '2024-11-12' and to_date(sysdate())
)

, morpho_ethereum_seed_funding as (
    select
        date(block_timestamp) as date
        , max(user_address) as user_address
        , max(contract_address) as contract_address
        , max_by(balance / 1e18, date) as balance
    from ethereum_flipside.core.fact_token_balances
    where
        lower(contract_address) = lower('0x58D97B57BB95320F9a05dC918Aef65434969c2B2')
        and lower(user_address) = lower('0x6ABfd6139c7C3CC270ee2Ce132E309F59cAaF6a2')
    group by date
)

, backfilled_seed_funding as (
    select
        d.date
        , last_value(m.user_address ignore nulls) over (
            order by d.date
            rows between unbounded preceding and current row
        ) as user_address
        , last_value(m.contract_address ignore nulls) over (
            order by d.date
            rows between unbounded preceding and current row
        ) as contract_address
        , last_value(m.balance ignore nulls) over (
            order by d.date
            rows between unbounded preceding and current row
        ) as balance
    from date_spine d
    left join morpho_ethereum_seed_funding m on m.date = d.date
)

, seed_funding_change as (
    select
        date
        , user_address
        , contract_address
        , balance
        , lead(balance) over (order by date desc) - balance as seed_funding_change
    from backfilled_seed_funding
)

select 
    date
    , user_address
    , contract_address
    , balance
    , seed_funding_change
from seed_funding_change
where date >= '2024-11-21'
order by date desc