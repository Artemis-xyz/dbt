{{
    config(
        materialized='table',
        snowflake_warehouse='AERODROME',
        database='aerodrome',
        schema='raw',
        alias='fact_aerodrome_supply_data'
    )
}}

with locked_supply_balance as (
    select 
        date(block_timestamp) as date, 
        balance_token / 1e18 as ve_aero_balance, 
        row_number() over (partition by date(block_timestamp) order by block_timestamp) as rn
    from base.prod_raw.ez_address_balances_by_token
    where lower(contract_address) = lower('0x940181a94A35A4569E4529A3CDfB74e38FD98631')
        and lower(address) = lower('0xeBf418Fe2512e7E6bd9b87a8F0f294aCDC67e6B4')
),

emissions as (
    select 
        date(block_timestamp) as date, 
        sum(amount) as emissions
    from base_flipside.core.ez_token_transfers
    where lower(contract_address) = lower('0x940181a94A35A4569E4529A3CDfB74e38FD98631')
        and lower(from_address) = lower('0x0000000000000000000000000000000000000000')
    group by date
)

select 
    coalesce(em.date, lsb.date) as date, 
    case when em.date = '2023-08-28' then coalesce(em.emissions, 0) else 0 end as pre_mine_unlocks, 
    case when em.date <> '2023-08-28' then coalesce(em.emissions, 0) else 0 end as emissions_native, 
    coalesce(lsb.ve_aero_balance, 0) as locked_supply, 
    sum(coalesce(em.emissions, 0)) over (
        order by em.date
        rows between unbounded preceding and current row
    ) as total_supply,
    sum(coalesce(em.emissions, 0)) over (
        order by em.date
        rows between unbounded preceding and current row
    ) - coalesce(lsb.ve_aero_balance, 0) as circulating_supply
from emissions as em
full join locked_supply_balance as lsb
    on em.date = lsb.date
where lsb.rn = 1
order by date asc