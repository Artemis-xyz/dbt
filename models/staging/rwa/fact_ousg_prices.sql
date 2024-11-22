{{
    config(
        materialized="table",
        snowflake_warehouse="RWA",
    )
}}

select date(hour) as date, avg(price) as price from ethereum_flipside.price.ez_prices_hourly
where symbol = 'OUSG'
group by 1