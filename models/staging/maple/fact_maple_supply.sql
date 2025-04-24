{{ 
    config(
        materialized="table",
        snowflake_warehouse="MAPLE",
    )
}}

with historical_syrup_supply as (
    select 
        date, 
        premine_unlocks_native, 
        circulating_supply_native
    from {{ ref("maple_daily_supply_data") }}
    order by date desc
), 

date_spine as (
    select date
    from {{ ref("dim_date_spine") }}
    where date < to_date(sysdate()) and date >= (select min(date) from historical_syrup_supply)
),

syrup_emissions as (
    select
        ds.date, 
        sum(coalesce(tt.amount, 0)) as emissions_native
    from date_spine as ds
    left join {{ source('ETHEREUM_FLIPSIDE', 'ez_token_transfers') }} as tt
        on date(tt.block_timestamp) = ds.date
    where lower(tt.contract_address) = lower('0x643C4E15d7d62Ad0aBeC4a9BD4b001aA3Ef52d66')
        and lower(from_address) = lower('0x0000000000000000000000000000000000000000')
        and lower(ez_token_transfers_id) <> lower('f68e204124a14bb7a1c65cd8c6c2103b') --excluding the 1B $SYRUP mint because that's just the replacement to $MPL
    group by date
    order by date desc
), 

emissions_filled as (
    select 
        date,
        coalesce(emissions_native, 0) as emissions_native
    from date_spine
    left join syrup_emissions using (date)
),

cumulative_emissions as (
    select
        date,
        sum(emissions_native) over (
            order by date
            rows between unbounded preceding and current row
        ) as cumulative_emissions_native
    from emissions_filled
),

locked_syrup as (
    select
        date, 
        sum(balance_native) as locked_syrup_native
    from {{ref('fact_maple_address_balances')}}
    where lower(contract_address) = lower('0x643C4E15d7d62Ad0aBeC4a9BD4b001aA3Ef52d66')
    group by date
), 

syrup_buybacks as (
    with partitioned_transfers as (
        select
            date(block_timestamp) as date, 
            tx_hash, 
            amount,
            amount_usd, 
            symbol, 
            row_number() over (partition by tx_hash order by symbol asc) as rn
        from {{ source('ETHEREUM_FLIPSIDE', 'ez_token_transfers') }}
        where (lower(to_address) = lower('0xa7cc8d3e64ea81670181b005a476d0ca46e4c1fc') 
                and lower(from_address) = lower('0x9008d19f58aabd9ed0d60971565aa8510560ab41') 
                and lower(symbol) = 'syrup')
            or (lower(from_address) = lower('0xa7cc8d3e64ea81670181b005a476d0ca46e4c1fc') 
                and lower(to_address) = lower('0x9008d19f58aabd9ed0d60971565aa8510560ab41') 
                and lower(symbol) = 'usdc')
    )
    
    select
        date, 
        sum(amount) as buybacks_native, 
        sum(amount_usd) as buybacks
    from partitioned_transfers
    where tx_hash in (
        select tx_hash
        from partitioned_transfers
        group by tx_hash
        having 
            count(case when rn = 1 and lower(symbol) = 'syrup' then 1 end) > 0 and
            count(case when rn = 2 and lower(symbol) = 'usdc' then 1 end) > 0
    )
        and rn = 1
    group by date
)

select 
    date,
    coalesce(premine_unlocks_native, 0) as premine_unlocks_native,
    coalesce(locked_syrup_native, 0) as locked_syrup_native,
    case 
        when date < '2024-09-12' then coalesce(circulating_supply_native, 0)
        when date >= '2024-09-12' then coalesce(1000000000 + cumulative_emissions_native, 0) - coalesce(locked_syrup_native, 0)
    end as circulating_supply_native,
    coalesce(emissions_native, 0) as emissions_native, 
    coalesce(buybacks_native, 0) as buybacks_native, 
    coalesce(buybacks, 0) as buybacks
from date_spine
left join historical_syrup_supply using (date)
left join emissions_filled using (date)
left join cumulative_emissions using (date)
left join locked_syrup using (date)
left join syrup_buybacks using (date)

