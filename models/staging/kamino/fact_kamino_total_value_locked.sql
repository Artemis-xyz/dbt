{{ config(materialized="table", snowflake_warehouse="KAMINO") }}

with reserve_balances as (
    select 
        tb.block_timestamp,
        tb.owner, 
        tb.mint, 
        tb.balance,  
        row_number() over (
            partition by date_trunc('day', tb.block_timestamp), tb.owner, tb.mint
            order by tb.block_timestamp desc
        ) as rn
    from solana_flipside.core.fact_token_balances as tb
    where tb.owner in ('9DrvZvyWh1HuAoZxvYWMvkf2XCzryCpGgHqrMjyDWpmo',  --Kamino Reserve 1
                                'B9spsrMK6pJicYtukaZzDyzsUQLgc3jbx5gHVwdDxb6y', --Kamino Reserve 2
                                '81BgcfZuZf9bESLvw3zDkh7cZmMtDwTPgkCvYu7zx26o', --Kamino Reserve 3
                                'GuWEkEJb5bh8Ai2gaYmZWMTUq8MrFeoaDZ89BrQfB1FZ', --Kamino Reserve 4
                                'Dx8iy2o46sK1DzWbEcznqSKeLbLVeu7otkibA3WohGAj'  --Kamino Reserve 5 
                                )
     order by tb.block_timestamp desc 
) 

    select
        date_trunc('day', rb.block_timestamp) as date,
        sum((rb.balance * eph.price)) as total_value_locked
    from reserve_balances as rb
    inner join solana_flipside.price.ez_prices_hourly as eph
        on eph.token_address = rb.mint and eph.hour = date_trunc('hour', rb.block_timestamp)
    where rb.rn = 1  
    group by date
    order by date desc

    