{{
    config(
        materialized='table',
        snowflake_warehouse='AERODROME',
        database='aerodrome',
        schema='raw',
        alias='fact_aerodrome_supply_side'
    )
}}

with locked_supply_balance as (
    select 
        date(block_timestamp) as date, 
        balance_token / 1e18 as ve_aero_balance, 
        row_number() over (partition by date(block_timestamp) order by block_timestamp) as rn, 
    from base.prod_raw.ez_address_balances_by_token
    where lower(contract_address) = lower('0x940181a94A35A4569E4529A3CDfB74e38FD98631')
        and lower(address) = lower('0xeBf418Fe2512e7E6bd9b87a8F0f294aCDC67e6B4')
),

emissions as (
    select 
        date(block_timestamp) as date, 
        sum(amount) as emissions, 
    from base_flipside.core.ez_token_transfers
    where lower(contract_address) = lower('0x940181a94A35A4569E4529A3CDfB74e38FD98631')
        and lower(from_address) = lower('0x0000000000000000000000000000000000000000')
    group by date
)

select 
    coalesce(em.date, lsb.date) as date, 
    coalesce(em.emissions, 0) as emissions, 
    coalesce(lsb.ve_aero_balance, 0) as locked_supply, 
    sum(emissions) over (
        order by em.date
        rows between unbounded preceding and current row
    ) as total_supply, 
    coalesce(total_supply,0) - coalesce(locked_supply,0) as circulating_supply
from emissions as em
full join locked_supply_balance as lsb
    on em.date = lsb.date
where lsb.rn = 1
order by date
