{{ config(
    materialized='table',
    snowflake_warehouse='VIRTUALS'
) }}

with virtuals_sablier_lockup as (
    select 
        date(block_timestamp) as date
        , max_by(balance_token, block_timestamp) / 1e18 as balance_token
    from {{ ref("fact_ethereum_address_balances_by_token") }}
    where lower(address) = lower('0xAFb979d9afAd1aD27C5eFf4E27226E3AB9e5dCC9')
    and lower(contract_address) = lower('0x44ff8620b8cA30902395A7bD3F2407e1A091BF73')
    group by date
    order by date desc
)

, virtuals_treasury as (
    select 
        date(block_timestamp) as date
        , max_by(balance_token, block_timestamp) / 1e18 as balance_token
    from {{ ref("fact_ethereum_address_balances_by_token") }}
    where lower(address) = lower('0x37672dDa85f3cB8dA4098bAAc5D84E00960Cb081')
    and lower(contract_address) = lower('0x44ff8620b8cA30902395A7bD3F2407e1A091BF73')
    group by date
    order by date desc
)

, date_spine as (
    select date
    from {{ ref("dim_date_spine") }}
    where date between (select min(date) from virtuals_sablier_lockup) and to_date(sysdate())
)

, filled_sablier_lockup as (
    select ds.date, vs.balance_token,
        row_number() over (partition by ds.date order by vs.date desc) as rn
    from date_spine ds
    left join virtuals_sablier_lockup vs on ds.date >= vs.date
)

, filled_treasury as (
    select ds.date, vs.balance_token,
        row_number() over (partition by ds.date order by vs.date desc) as rn
    from date_spine ds
    left join virtuals_treasury vs on ds.date >= vs.date
)

, virtuals_supply_data as (
    select 
        s.date,
        s.balance_token as virtuals_sablier_lockup,
        t.balance_token as virtuals_treasury
    from (select * from filled_sablier_lockup where rn = 1) s
    join (select * from filled_treasury where rn = 1) t using (date)
)

, deltas as (
    select *,
        coalesce(virtuals_sablier_lockup - lag(virtuals_sablier_lockup) over (order by date), 0) as delta_sablier,
        coalesce(virtuals_treasury - lag(virtuals_treasury) over (order by date), 0) as delta_treasury
    from virtuals_supply_data
)

, virtuals_circulating_supply as (
    select
        date
        , virtuals_sablier_lockup
        , virtuals_treasury
        , virtuals_sablier_lockup + virtuals_treasury as premine_unlocks_native
        , premine_unlocks_native as net_supply_change_native
        , 1000000000 - (virtuals_sablier_lockup + virtuals_treasury) as circulating_supply_native
    from virtuals_supply_data
)
select 
    date
    , virtuals_sablier_lockup
    , virtuals_treasury
    , case
        -- if the delta is negative (because the balance is decreasing), people are claiming from the sablier lockup or the treasury
        when delta_sablier < 0 or delta_treasury < 0
        then (delta_sablier * -1) + (delta_treasury * -1)
        else 0
    end as premine_unlocks_native
    , case
        -- if the delta is negative (because the balance is decreasing), people are claiming from the sablier lockup or the treasury
        when delta_sablier < 0 or delta_treasury < 0
        then (delta_sablier * -1) + (delta_treasury * -1)
        else 0
    end as net_supply_change_native
    , 1000000000 - (virtuals_sablier_lockup + virtuals_treasury) as circulating_supply_native
from deltas
order by date