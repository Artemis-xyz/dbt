{{
    config(
        materialized='table',
        snowflake_warehouse='AERODROME',
        database='aerodrome',
        schema='raw',
        alias='fact_aerodrome_supply_data'
    )
}}

with locked_supply_balance_partitioned as (
    select 
        date(block_timestamp) as date, 
        balance_token / 1e18 as ve_aero_balance, 
        row_number() over (partition by date(block_timestamp) order by block_timestamp) as rn
    from {{ ref('ez_base_address_balances_by_token') }}
    where lower(contract_address) = lower('0x940181a94A35A4569E4529A3CDfB74e38FD98631')
        and lower(address) = lower('0xeBf418Fe2512e7E6bd9b87a8F0f294aCDC67e6B4')
),

locked_supply_balance as (
    select 
        date, 
        ve_aero_balance
    from locked_supply_balance_partitioned
    where rn = 1
),

emissions as (
    select 
        date(block_timestamp) as date, 
        sum(amount) as emissions
    from {{ source('BASE_FLIPSIDE', 'ez_token_transfers') }}
    where lower(contract_address) = lower('0x940181a94A35A4569E4529A3CDfB74e38FD98631')
        and lower(from_address) = lower('0x0000000000000000000000000000000000000000')
    group by date
), 

buybacks as (
    select
        date(block_timestamp) as date, 
        sum(amount) as buybacks_native, 
        sum(amount_usd) as buybacks
    from {{ source('BASE_FLIPSIDE', 'ez_token_transfers') }}
    where lower(from_address) in (lower('0x834C0DA026d5F933C2c18Fa9F8Ba7f1f792fDa52'), 
                                    lower('0xc27c8B3Ce02349f4916BFC8FD45A586D8787Ee5e'), 
                                    lower('0xc9814f18a8751214F719De15C54D01b3D78EF14f')
                                )
        and lower(to_address) = lower('0xeBf418Fe2512e7E6bd9b87a8F0f294aCDC67e6B4')
    group by date 
), 

date_spine as (
    select date
    from {{ ref('dim_date_spine') }}
    where date >= '2023-08-28' and date <= current_date
)

select 
    ds.date, 
    case when ds.date = '2023-08-28' then coalesce(em.emissions, 0) else 0 end as pre_mine_unlocks, 
    case when ds.date <> '2023-08-28' then coalesce(em.emissions, 0) else 0 end as emissions_native, 
    coalesce(lsb.ve_aero_balance, 0) as locked_supply, 
    sum(coalesce(em.emissions, 0)) over (
        order by ds.date
        rows between unbounded preceding and current row
    ) as total_supply,
    sum(coalesce(em.emissions, 0)) over (
        order by ds.date
        rows between unbounded preceding and current row
    ) - coalesce(lsb.ve_aero_balance, 0) as circulating_supply_native,
    coalesce(bb.buybacks_native, 0) as buybacks_native, 
    coalesce(bb.buybacks, 0) as buybacks
from date_spine as ds
full join emissions as em
    on ds.date = em.date
full join locked_supply_balance as lsb
    on ds.date = lsb.date
full join buybacks as bb
    on ds.date = bb.date
order by ds.date asc