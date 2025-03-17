

with ordered_transactions as (
    select distinct 
        tb.block_timestamp, 
        tb.tx_id, 
        tb.owner, 
        eph.price,
        eph.symbol,
        (tb.balance * eph.price) as value_locked_for_token, 
        row_number() over (
            partition by tb.mint, tb.owner, date_trunc('day', tb.block_timestamp)
            order by tb.block_timestamp desc
        ) as rn 
    from solana_flipside.core.fact_token_balances as tb
    inner join solana_flipside.price.ez_prices_hourly as eph
        on tb.mint = eph.token_address and date_trunc('hour', tb.block_timestamp) = eph.hour 
    where tb.owner in ('9DrvZvyWh1HuAoZxvYWMvkf2XCzryCpGgHqrMjyDWpmo',  //Kamino Reserve 1
                                'B9spsrMK6pJicYtukaZzDyzsUQLgc3jbx5gHVwdDxb6y', //Kamino Reserve 2
                                '81BgcfZuZf9bESLvw3zDkh7cZmMtDwTPgkCvYu7zx26o', //Kamino Reserve 3
                                'GuWEkEJb5bh8Ai2gaYmZWMTUq8MrFeoaDZ89BrQfB1FZ', //Kamino Reserve 4
                                'Dx8iy2o46sK1DzWbEcznqSKeLbLVeu7otkibA3WohGAj'  //Kamino Reserve 5 
                                ) and date_trunc('day', tb.block_timestamp) > '2025-02-19' 
    order by tb.block_timestamp desc 
),

individual_fees as (
    select 
        to_date(substr(flat.value:date::string, 1, 10), 'yyyy-mm-dd') as datestamp,
        flat.value:mint_address::string as mint_address, 
        flat.value:accumulated_fees::float as accumulated_fees, 
        (eph.price * accumulated_fees) as fee_amounts
    from landing_database.prod_landing.raw_kamino_accumulated_fees as af,
           lateral flatten(input => source_json) flat
    inner join solana_flipside.price.ez_prices_hourly as eph
        on mint_address = eph.token_address and datestamp = date_trunc('day', eph.hour)
    where substr(eph.hour, 12) = '00:00:00.000' and substr(extraction_date, 12) = '16:06:55.257'
), 

cumulative_fees as (
    select
        indi.datestamp, 
        sum(indi.fee_amounts) as sum_fees
    from individual_fees as indi
    group by datestamp
    order by datestamp desc
), 

total_liquidity as (
    select
        date_trunc('day', ot.block_timestamp) as date_timestamp,
        sum(ot.value_locked_for_token) as total_liquidity
    from ordered_transactions as ot
    where ot.rn = 1
    group by date_timestamp
    order by date_timestamp desc
)

select
    tl.date_timestamp, 
    (tl.total_liquidity - cf.sum_fees) as total_value_locked
from total_liquidity as tl
join cumulative_fees as cf
    on tl.date_timestamp = cf.datestamp
    