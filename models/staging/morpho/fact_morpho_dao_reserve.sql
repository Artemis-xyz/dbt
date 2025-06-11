{{ config(materialized="table", snowflake_warehouse="MEDIUM") }}


with date_spine as (
    select 
        date
    from {{ ref("dim_date_spine") }}
    where date between '2024-11-12' and to_date(sysdate())
)

, morpho_ethereum_dao_reserve as (
    select
        date(block_timestamp) as date
        , user_address
        , contract_address
        , balance / 1e18 as balance
        , 'ethereum' as chain
    from ethereum_flipside.core.fact_token_balances
    where
        lower(contract_address) = lower('0x58D97B57BB95320F9a05dC918Aef65434969c2B2')
        and lower(user_address) = lower('0xcBa28b38103307Ec8dA98377ffF9816C164f9AFa')
        and date(block_timestamp) >= '2024-11-12'
)

, backfilled_balances AS (
    select
        d.date
        , last_value(m.user_address ignore nulls) over (
            order by d.date 
            rows between unbounded preceding and current row
        ) as user_address,
        last_value(m.contract_address ignore nulls) over (
            order by d.date 
            rows between unbounded preceding and current row
        ) as contract_address,
        last_value(m.balance ignore nulls) over (
            order by d.date 
            rows between unbounded preceding and current row
        ) as balance
    from date_spine d
    left join morpho_ethereum_dao_reserve m on m.date = d.date
)

, dao_reserve_change as (
    select
        date
        , user_address
        , contract_address
        , balance
        , lead(balance) over (order by date desc) - balance as dao_reserve_change
    from backfilled_balances
)

select
    date
    , user_address
    , contract_address
    , balance
    , dao_reserve_change
from dao_reserve_change
where date >= '2024-11-21'
order by date desc